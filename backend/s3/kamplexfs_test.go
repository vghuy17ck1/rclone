package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kamplexfsAllFeatures = `{"version":"1.2.3","storageMode":"packed-volume","maxKeys":2500,"unknownField":1,
"features":["rename-object","rename-prefix","sequential-multipart","mtime-nanos","checksums","some-future-feature"]}`

// kamplexfsRequest is a request received by the fake server
type kamplexfsRequest struct {
	Method string
	Path   string // escaped path
	Query  string
	Header http.Header
}

// kamplexfsFake is a fake KamPlexFS S3 server mounted under /s3/
type kamplexfsFake struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	requests []kamplexfsRequest

	// responses, keyed by the query operation
	capsStatus   int
	capsBody     string
	renameStatus int
	renameCode   string
	prefixStatus int
	prefixCode   string
}

func newKamPlexFSFake(t *testing.T, tls bool) *kamplexfsFake {
	k := &kamplexfsFake{
		t:            t,
		capsStatus:   http.StatusOK,
		capsBody:     kamplexfsAllFeatures,
		renameStatus: http.StatusOK,
		prefixStatus: http.StatusOK,
	}
	if tls {
		k.srv = httptest.NewTLSServer(k)
	} else {
		k.srv = httptest.NewServer(k)
	}
	t.Cleanup(k.srv.Close)
	return k
}

func (k *kamplexfsFake) endpoint() string {
	return k.srv.URL + "/s3/"
}

func (k *kamplexfsFake) reset() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.requests = nil
}

// find returns the requests with the query operation op
func (k *kamplexfsFake) find(op string) (out []kamplexfsRequest) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, r := range k.requests {
		if strings.Contains("&"+r.Query+"&", "&"+op+"&") || strings.Contains("&"+r.Query, "&"+op+"=") {
			out = append(out, r)
		}
	}
	return out
}

func (k *kamplexfsFake) count(method string) (n int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, r := range k.requests {
		if r.Method == method {
			n++
		}
	}
	return n
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>test %s</Message></Error>`, code, code)
}

func (k *kamplexfsFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	k.requests = append(k.requests, kamplexfsRequest{
		Method: r.Method,
		Path:   r.URL.EscapedPath(),
		Query:  r.URL.RawQuery,
		Header: r.Header.Clone(),
	})
	k.mu.Unlock()
	if !strings.HasPrefix(r.URL.Path, "/s3/") {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket")
		return
	}
	q := r.URL.Query()
	switch {
	case q.Has("kamplexfsCapabilities"):
		if k.capsStatus != http.StatusOK {
			writeS3Error(w, k.capsStatus, "NotImplemented")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(k.capsBody))
	case q.Has("renameObject"):
		if k.renameStatus != http.StatusOK {
			writeS3Error(w, k.renameStatus, k.renameCode)
			return
		}
		w.WriteHeader(http.StatusOK)
	case q.Has("kamplexfsRenamePrefix"):
		if k.prefixStatus != http.StatusOK {
			writeS3Error(w, k.prefixStatus, k.prefixCode)
			return
		}
		_, _ = w.Write([]byte(`<RenamePrefixResult><ObjectsRenamed>42</ObjectsRenamed></RenamePrefixResult>`))
	case q.Has("uploads"):
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`))
	case r.Method == http.MethodHead:
		writeS3Error(w, http.StatusNotFound, "NotFound")
	default:
		// bucket creation and anything else
		w.WriteHeader(http.StatusOK)
	}
}

// newKamPlexFSTestFs makes an Fs talking to the fake
func newKamPlexFSTestFs(ctx context.Context, t *testing.T, endpoint, provider string, extra configmap.Simple) *Fs {
	regInfo, err := fs.Find("s3")
	require.NoError(t, err)
	cfg := configmap.Simple{
		"provider":          provider,
		"endpoint":          endpoint,
		"access_key_id":     "AKID",
		"secret_access_key": "SECRET",
		"no_check_bucket":   "true",
	}
	for k, v := range extra {
		cfg[k] = v
	}
	m := fs.ConfigMap("s3", regInfo.Options, "TestKamPlexFS", cfg)
	f, err := NewFs(ctx, "TestKamPlexFS", "bucket", m)
	require.NoError(t, err)
	return f.(*Fs)
}

func TestKamPlexFSCapabilities(t *testing.T) {
	ctx := context.Background()

	t.Run("Present", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)

		caps := k.find("kamplexfsCapabilities")
		require.Len(t, caps, 1)
		assert.Equal(t, "GET", caps[0].Method)
		assert.Equal(t, "/s3/", caps[0].Path, "must keep the endpoint path prefix")
		assert.True(t, strings.HasPrefix(caps[0].Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/"), "must be SigV4 signed")

		require.NotNil(t, f.kpx)
		assert.Equal(t, "packed-volume", f.kpx.caps.StorageMode)
		assert.True(t, f.kpx.has(kamplexfsFeatureRenameObject))
		assert.True(t, f.kpx.has(kamplexfsFeatureRenamePrefix))
		assert.NotNil(t, f.Features().Move)
		assert.NotNil(t, f.Features().DirMove)
		assert.Equal(t, int32(2500), f.opt.ListChunk)
		assert.True(t, f.opt.NoHead)
		assert.Equal(t, 1, f.opt.UploadConcurrency)
		// checksums are only enabled over https
		assert.False(t, f.opt.UseDataIntegrityProtections.Value)
		assert.Equal(t, aws.RequestChecksumCalculationWhenRequired, f.c.Options().RequestChecksumCalculation)
	})

	failures := []struct {
		name  string
		setup func(k *kamplexfsFake) string
	}{{
		name: "Absent",
		setup: func(k *kamplexfsFake) string {
			k.capsStatus = http.StatusNotFound
			return k.endpoint()
		},
	}, {
		name: "Garbage",
		setup: func(k *kamplexfsFake) string {
			k.capsBody = "<html>not json</html>"
			return k.endpoint()
		},
	}, {
		name: "NetworkError",
		setup: func(k *kamplexfsFake) string {
			k.srv.Close()
			return k.endpoint()
		},
	}}
	for _, test := range failures {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			endpoint := test.setup(k)
			f := newKamPlexFSTestFs(ctx, t, endpoint, kamplexfsProvider, nil)
			require.NotNil(t, f.kpx)
			assert.Empty(t, f.kpx.features)
			assert.Nil(t, f.Features().Move)
			assert.Nil(t, f.Features().DirMove)
			assert.Equal(t, int32(1000), f.opt.ListChunk)
			assert.True(t, f.opt.NoHead)
			assert.Equal(t, time.Second, f.Precision())
		})
	}

	t.Run("UserOverrides", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{
			"no_head":            "false",
			"list_chunk":         "100",
			"upload_concurrency": "3",
		})
		assert.False(t, f.opt.NoHead)
		assert.Equal(t, int32(100), f.opt.ListChunk)
		assert.Equal(t, 3, f.opt.UploadConcurrency)
	})
}

func TestKamPlexFSChecksumsOverHTTPS(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.InsecureSkipVerify = true
	k := newKamPlexFSFake(t, true)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	require.Len(t, k.find("kamplexfsCapabilities"), 1)
	assert.True(t, f.opt.UseDataIntegrityProtections.Value)
	assert.Equal(t, aws.RequestChecksumCalculationWhenSupported, f.c.Options().RequestChecksumCalculation)

	// Without the capability they stay off
	k.capsBody = `{"features":[]}`
	f = newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	assert.False(t, f.opt.UseDataIntegrityProtections.Value)
	assert.Equal(t, aws.RequestChecksumCalculationWhenRequired, f.c.Options().RequestChecksumCalculation)
}

func TestKamPlexFSOtherProvidersUnchanged(t *testing.T) {
	ctx := context.Background()
	for _, provider := range []string{"Other", "Minio", "Rclone"} {
		t.Run(provider, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), provider, nil)
			assert.Nil(t, f.kpx)
			assert.Empty(t, k.find("kamplexfsCapabilities"), "must not ask for capabilities")
			assert.Nil(t, f.Features().Move)
			assert.Nil(t, f.Features().DirMove)
			assert.True(t, f.Features().SlowModTime)
			assert.Equal(t, time.Nanosecond, f.Precision())
			assert.False(t, f.opt.NoHead)
			assert.Equal(t, 4, f.opt.UploadConcurrency)
			assert.Equal(t, int32(1000), f.opt.ListChunk)
			mtime := time.Unix(1709608272, 123456789)
			assert.Equal(t, mtime, f.kamplexfsTruncateMtime(mtime))
		})
	}
}

func TestKamPlexFSModTime(t *testing.T) {
	ctx := context.Background()
	listed := time.Unix(1709608272, 0).UTC()

	t.Run("Listing", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		// kamplexfs_exact_modtime is ignored without mtime-nanos
		k.capsBody = `{"features":["rename-object"]}`
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{
			"kamplexfs_exact_modtime": "true",
		})
		assert.Equal(t, time.Second, f.Precision())
		assert.False(t, f.Features().SlowModTime)

		k.reset()
		o := &Object{fs: f, remote: "file.txt", lastModified: listed}
		assert.Equal(t, listed, o.ModTime(ctx))
		assert.Equal(t, 0, k.count("HEAD"), "must not HEAD the object")

		// mtime metadata from an upload is preferred when known
		o.meta = map[string]string{metaMtime: "1709608273.5"}
		assert.Equal(t, time.Unix(1709608273, 500000000), o.ModTime(ctx))
		assert.Equal(t, 0, k.count("HEAD"))
	})

	t.Run("Exact", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{
			"kamplexfs_exact_modtime": "true",
		})
		assert.Equal(t, time.Nanosecond, f.Precision())
		assert.True(t, f.Features().SlowModTime)

		k.reset()
		o := &Object{fs: f, remote: "file.txt", lastModified: listed}
		_ = o.ModTime(ctx)
		assert.Equal(t, 1, k.count("HEAD"), "must read the metadata with HEAD")
	})

	t.Run("NotExactByDefault", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
		assert.Equal(t, time.Second, f.Precision())
		assert.False(t, f.Features().SlowModTime)
	})
}

func TestKamPlexFSUploadMtime(t *testing.T) {
	ctx := context.Background()
	mtime := time.Unix(1709608272, 123456700)
	for _, test := range []struct {
		name string
		caps string
		want string
	}{
		{"OldServer", `{"features":[]}`, "1709608272"},
		{"MtimeNanos", `{"features":["mtime-nanos"]}`, "1709608272.1234567"},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			k.capsBody = test.caps
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
			o := &Object{fs: f, remote: "file.txt"}
			src := object.NewStaticObjectInfo("file.txt", mtime, 1, true, nil, nil)
			ui, err := o.prepareUpload(ctx, src, nil, true)
			require.NoError(t, err)
			assert.Equal(t, test.want, ui.req.Metadata[metaMtime])
		})
	}
}

func TestKamPlexFSOpenChunkWriterConcurrency(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name  string
		extra configmap.Simple
		want  int
	}{
		{"SequentialDefault", nil, 1},
		{"SequentialOff", configmap.Simple{"kamplexfs_sequential_upload": "false"}, 4},
		{"ExplicitConcurrency", configmap.Simple{"upload_concurrency": "3"}, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, test.extra)
			src := object.NewStaticObjectInfo("file.txt", time.Now(), 100*1024*1024, true, nil, nil)
			info, _, err := f.OpenChunkWriter(ctx, "file.txt", src)
			require.NoError(t, err)
			assert.Equal(t, test.want, info.Concurrency)
		})
	}
}

func TestKamPlexFSMove(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSFake(t, false)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	mtime := time.Unix(1709608272, 0)
	src := &Object{
		fs:           f,
		remote:       "dir/src file+1.txt",
		md5:          "45eb1102ca142e0ba30438cf38c7ea1b",
		bytes:        42,
		lastModified: mtime,
		mimeType:     "text/plain",
		meta:         map[string]string{metaMtime: "1709608272"},
	}

	t.Run("OK", func(t *testing.T) {
		k.reset()
		dst, err := f.Features().Move(ctx, src, "other/dst file.txt")
		require.NoError(t, err)
		reqs := k.find("renameObject")
		require.Len(t, reqs, 1)
		r := reqs[0]
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/s3/bucket/other/dst%20file.txt", r.Path)
		assert.Equal(t, "/bucket/dir/src%20file%2B1.txt", r.Header.Get("X-Amz-Rename-Source"))
		assert.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 "))
		assert.Empty(t, r.Header.Get("If-None-Match"), "must overwrite the destination")
		assert.Equal(t, 0, k.count("HEAD"), "must not HEAD the new object")

		assert.Equal(t, "other/dst file.txt", dst.Remote())
		assert.Equal(t, int64(42), dst.Size())
		assert.Equal(t, mtime, dst.ModTime(ctx))
		md5, err := dst.Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Equal(t, src.md5, md5)
	})

	t.Run("NotSameRemote", func(t *testing.T) {
		k.reset()
		other := &Object{fs: &Fs{opt: Options{Endpoint: "http://elsewhere/"}}, remote: "x"}
		_, err := f.Features().Move(ctx, other, "y")
		assert.Equal(t, fs.ErrorCantMove, err)
		assert.Empty(t, k.find("renameObject"))
	})

	for _, test := range []struct {
		status int
		code   string
		want   error
	}{
		{http.StatusNotFound, "NoSuchKey", fs.ErrorObjectNotFound},
		{http.StatusBadRequest, "InvalidRequest", fs.ErrorCantMove},
		{http.StatusNotImplemented, "NotImplemented", fs.ErrorCantMove},
		{http.StatusConflict, "KeyCollision", nil},
	} {
		t.Run(fmt.Sprintf("Error%d%s", test.status, test.code), func(t *testing.T) {
			k.reset()
			k.renameStatus, k.renameCode = test.status, test.code
			defer func() { k.renameStatus = http.StatusOK }()
			_, err := f.Features().Move(ctx, src, "dst.txt")
			require.Error(t, err)
			if test.want != nil {
				assert.Equal(t, test.want, err)
			} else {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, test.code, apiErr.ErrorCode())
				assert.Len(t, k.find("renameObject"), 1, "must not retry")
			}
		})
	}
}

func TestKamPlexFSDirMove(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSFake(t, false)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)

	for _, test := range []struct {
		name      string
		srcRoot   string
		srcRemote string
		dstRoot   string
		dstRemote string
		wantPath  string
		wantSrc   string
		wantRmdir bool // the source bucket is removed
	}{
		{"Subdirs", "bucket", "a b", "bucket", "c+d", "/s3/bucket/c%2Bd/", "/bucket/a%20b/", false},
		{"Roots", "bucket/src", "", "bucket2/dst", "", "/s3/bucket2/dst/", "/bucket/src/", false},
		{"WholeBuckets", "bucket", "", "bucket2", "", "/s3/bucket2/", "/bucket/", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			srcFs := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
			srcFs.setRoot(test.srcRoot)
			f.setRoot(test.dstRoot)
			k.reset()
			err := f.Features().DirMove(ctx, srcFs, test.srcRemote, test.dstRemote)
			require.NoError(t, err)
			reqs := k.find("kamplexfsRenamePrefix")
			require.Len(t, reqs, 1)
			r := reqs[0]
			assert.Equal(t, "PUT", r.Method)
			assert.Equal(t, test.wantPath, r.Path)
			assert.Equal(t, test.wantSrc, r.Header.Get("X-Kamplexfs-Rename-Source"))
			assert.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 "))
			if test.wantRmdir {
				assert.Equal(t, 1, k.count("DELETE"), "must remove the empty source bucket")
			} else {
				assert.Equal(t, 0, k.count("DELETE"))
			}
		})
	}

	f.setRoot("bucket")
	srcFs := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)

	t.Run("NoBucket", func(t *testing.T) {
		k.reset()
		srcFs.setRoot("")
		defer srcFs.setRoot("bucket")
		assert.Equal(t, fs.ErrorCantDirMove, f.Features().DirMove(ctx, srcFs, "", "dst"))
		assert.Empty(t, k.find("kamplexfsRenamePrefix"))
	})

	t.Run("NotSameRemote", func(t *testing.T) {
		other := &Fs{opt: Options{Endpoint: "http://elsewhere/"}}
		assert.Equal(t, fs.ErrorCantDirMove, f.Features().DirMove(ctx, other, "src", "dst"))
	})

	for _, test := range []struct {
		status int
		code   string
		want   error
	}{
		{http.StatusConflict, "DestinationNotEmpty", fs.ErrorDirExists},
		{http.StatusNotFound, "NoSuchKey", fs.ErrorDirNotFound},
		{http.StatusBadRequest, "InvalidArgument", fs.ErrorCantDirMove},
		{http.StatusNotImplemented, "NotImplemented", fs.ErrorCantDirMove},
		{http.StatusConflict, "KeyCollision", nil},
	} {
		t.Run(fmt.Sprintf("Error%d%s", test.status, test.code), func(t *testing.T) {
			k.reset()
			k.prefixStatus, k.prefixCode = test.status, test.code
			defer func() { k.prefixStatus = http.StatusOK }()
			err := f.Features().DirMove(ctx, srcFs, "src", "dst")
			require.Error(t, err)
			if test.want != nil {
				assert.Equal(t, test.want, err)
			} else {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, test.code, apiErr.ErrorCode())
				assert.Len(t, k.find("kamplexfsRenamePrefix"), 1, "must not retry")
			}
		})
	}
}
