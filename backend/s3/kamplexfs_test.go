package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
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

// kamplexfsObject is an object stored by the fake server
type kamplexfsObject struct {
	size         int64
	etag         string
	lastModified time.Time
	meta         http.Header // the X-Amz-Meta-* headers
	blake3       string      // "" if not computed
}

// kamplexfsFake is a fake KamPlexFS S3 server mounted under /s3/
type kamplexfsFake struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	requests []kamplexfsRequest
	objects  map[string]*kamplexfsObject // keyed by bucket/key
	uploads  map[string]*kamplexfsUpload // multipart uploads keyed by bucket/key

	// hashing, as set by enable_md5 and enable_blake3
	noMD5         bool // ETags aren't MD5s
	blake3        bool // BLAKE3 digests are computed
	blake3Pending bool // BLAKE3 digests aren't computed inline
	blake3Wrong   bool // upload responses have a wrong BLAKE3

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
		objects:      map[string]*kamplexfsObject{},
		uploads:      map[string]*kamplexfsUpload{},
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
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/s3/"), "/")
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
		src, err := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Rename-Source"), "/"))
		require.NoError(k.t, err)
		k.mu.Lock()
		if o, ok := k.objects[src]; ok {
			delete(k.objects, src)
			k.objects[bucket+"/"+key] = o
		}
		k.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case q.Has("kamplexfsRenamePrefix"):
		if k.prefixStatus != http.StatusOK {
			writeS3Error(w, k.prefixStatus, k.prefixCode)
			return
		}
		_, _ = w.Write([]byte(`<RenamePrefixResult><ObjectsRenamed>42</ObjectsRenamed></RenamePrefixResult>`))
	case r.Method == http.MethodGet && key == "" && q.Has("kamplexfsHashes"):
		k.listHashes(w, r, bucket)
	case q.Get("x-id") == "UploadPart":
		k.uploadPart(w, r, bucket+"/"+key)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		k.completeUpload(w, bucket+"/"+key)
	case q.Has("uploads"):
		k.mu.Lock()
		k.uploads[bucket+"/"+key] = &kamplexfsUpload{meta: metaHeaders(r.Header), parts: map[int][]byte{}}
		k.mu.Unlock()
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`))
	case r.Method == http.MethodGet && key == "" && q.Get("list-type") == "2":
		k.list(w, bucket, q.Get("prefix"), q.Get("delimiter"))
	case q.Get("x-id") == "PutObject" || q.Get("x-id") == "CopyObject":
		k.put(w, r, bucket+"/"+key)
	case r.Method == http.MethodHead:
		k.mu.Lock()
		o, ok := k.objects[bucket+"/"+key]
		k.mu.Unlock()
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NotFound")
			return
		}
		for name, values := range o.meta {
			w.Header()[name] = values
		}
		w.Header().Set("Content-Length", strconv.FormatInt(o.size, 10))
		w.Header().Set("ETag", `"`+o.etag+`"`)
		if o.blake3 != "" {
			w.Header().Set(kamplexfsBlake3Header, o.blake3)
		}
		w.Header().Set("Last-Modified", o.lastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	default:
		// bucket creation and anything else
		w.WriteHeader(http.StatusOK)
	}
}

// put stores an uploaded or copied object as name
func (k *kamplexfsFake) put(w http.ResponseWriter, r *http.Request, name string) {
	o := &kamplexfsObject{meta: http.Header{}}
	source := r.Header.Get("X-Amz-Copy-Source")
	if source != "" {
		source, err := url.PathUnescape(strings.TrimPrefix(source, "/"))
		require.NoError(k.t, err)
		k.mu.Lock()
		src, ok := k.objects[source]
		k.mu.Unlock()
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		*o = *src
		o.meta = src.meta.Clone()
	} else {
		assert.Empty(k.t, r.Header.Get("X-Amz-Decoded-Content-Length"), "the fake can't read aws-chunked bodies")
		body, err := io.ReadAll(r.Body)
		require.NoError(k.t, err)
		k.setData(o, body)
	}
	if source == "" || r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE" {
		o.meta = metaHeaders(r.Header)
	}
	k.store(w, name, o, source != "")
}

// metaHeaders returns the X-Amz-Meta-* headers
func metaHeaders(header http.Header) http.Header {
	meta := http.Header{}
	for name, values := range header {
		if strings.HasPrefix(name, "X-Amz-Meta-") {
			meta[name] = values
		}
	}
	return meta
}

// blake3Hex returns the BLAKE3 of data
func blake3Hex(data []byte) string {
	mh, _ := hash.NewMultiHasherTypes(hash.NewHashSet(hash.BLAKE3))
	_, _ = mh.Write(data)
	return mh.Sums()[hash.BLAKE3]
}

// setData sets the size and hashes of o from its data as the server
// does with its hashing configuration.
func (k *kamplexfsFake) setData(o *kamplexfsObject, data []byte) {
	sum := md5.Sum(data)
	o.size, o.etag = int64(len(data)), hex.EncodeToString(sum[:])
	b3 := blake3Hex(data)
	if k.blake3 && !k.blake3Pending {
		o.blake3 = b3
	}
	if k.noMD5 {
		if k.blake3 {
			o.etag = "b3-" + b3
		} else {
			o.etag = "nohash-" + b3[:32]
		}
	}
}

// uploadHeaders sets the headers of an upload response for o
func (k *kamplexfsFake) uploadHeaders(w http.ResponseWriter, o *kamplexfsObject) {
	switch {
	case o.blake3 == "":
	case k.blake3Wrong:
		w.Header().Set(kamplexfsBlake3Header, strings.Repeat("0", 64))
	default:
		w.Header().Set(kamplexfsBlake3Header, o.blake3)
	}
}

// store stores o as name and writes the response
func (k *kamplexfsFake) store(w http.ResponseWriter, name string, o *kamplexfsObject, copied bool) {
	k.storeObject(o, name)
	if copied {
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"%s"</ETag><LastModified>%s</LastModified></CopyObjectResult>`,
			o.etag, o.lastModified.UTC().Format(time.RFC3339))
		return
	}
	k.uploadHeaders(w, o)
	w.Header().Set("ETag", `"`+o.etag+`"`)
	w.WriteHeader(http.StatusOK)
}

// storeObject stores o as name
//
// Like the server, it reports the mtime metadata floored to whole
// seconds as the LastModified.
func (k *kamplexfsFake) storeObject(o *kamplexfsObject, name string) {
	o.lastModified = time.Now().Truncate(time.Second)
	if secs, _, _ := strings.Cut(o.meta.Get("X-Amz-Meta-Mtime"), "."); secs != "" {
		n, err := strconv.ParseInt(secs, 10, 64)
		require.NoError(k.t, err)
		o.lastModified = time.Unix(n, 0)
	}
	k.mu.Lock()
	k.objects[name] = o
	k.mu.Unlock()
}

// kamplexfsUpload is a multipart upload in progress
type kamplexfsUpload struct {
	meta  http.Header
	parts map[int][]byte
}

// uploadPart stores a part of the multipart upload of name
func (k *kamplexfsFake) uploadPart(w http.ResponseWriter, r *http.Request, name string) {
	assert.Empty(k.t, r.Header.Get("X-Amz-Decoded-Content-Length"), "the fake can't read aws-chunked bodies")
	body, err := io.ReadAll(r.Body)
	require.NoError(k.t, err)
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	require.NoError(k.t, err)
	k.mu.Lock()
	k.uploads[name].parts[partNumber] = body
	k.mu.Unlock()
	sum := md5.Sum(body)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	w.WriteHeader(http.StatusOK)
}

// completeUpload joins the parts of the multipart upload of name
func (k *kamplexfsFake) completeUpload(w http.ResponseWriter, name string) {
	k.mu.Lock()
	upload := k.uploads[name]
	delete(k.uploads, name)
	k.mu.Unlock()
	var data []byte
	for _, partNumber := range slices.Sorted(maps.Keys(upload.parts)) {
		data = append(data, upload.parts[partNumber]...)
	}
	o := &kamplexfsObject{meta: upload.meta}
	k.setData(o, data)
	o.etag += fmt.Sprintf("-%d", len(upload.parts))
	k.uploadHeaders(w, o)
	bucket, key, _ := strings.Cut(name, "/")
	k.storeObject(o, name)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>"%s"</ETag></CompleteMultipartUploadResult>`,
		html.EscapeString(bucket), html.EscapeString(key), o.etag)
}

// listHashes is the bulk listing of the BLAKE3 digests
func (k *kamplexfsFake) listHashes(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix, delimiter, token := q.Get("prefix"), q.Get("delimiter"), q.Get("continuation-token")
	maxKeys, err := strconv.Atoi(q.Get("max-keys"))
	require.NoError(k.t, err)
	k.mu.Lock()
	defer k.mu.Unlock()
	var keys, dirs []string
	for name := range k.objects {
		key, ok := strings.CutPrefix(name, bucket+"/")
		if !ok || !strings.HasPrefix(key, prefix) || key <= token {
			continue
		}
		if i := strings.Index(key[len(prefix):], delimiter); delimiter != "" && i >= 0 {
			if dir := key[:len(prefix)+i+len(delimiter)]; !slices.Contains(dirs, dir) {
				dirs = append(dirs, dir)
			}
			continue
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	slices.Sort(dirs)
	type entry struct {
		Key          string  `json:"key"`
		Size         int64   `json:"size"`
		ETag         string  `json:"etag"`
		LastModified string  `json:"lastModified"`
		Blake3       *string `json:"blake3"`
	}
	resp := struct {
		Objects               []entry  `json:"objects"`
		CommonPrefixes        []string `json:"commonPrefixes"`
		NextContinuationToken *string  `json:"nextContinuationToken"`
	}{Objects: []entry{}, CommonPrefixes: dirs}
	for i, key := range keys {
		if i == maxKeys {
			resp.NextContinuationToken = &resp.Objects[i-1].Key
			break
		}
		o := k.objects[bucket+"/"+key]
		e := entry{Key: key, Size: o.size, ETag: o.etag, LastModified: o.lastModified.UTC().Format(time.RFC3339)}
		if o.blake3 != "" {
			e.Blake3 = &o.blake3
		}
		resp.Objects = append(resp.Objects, e)
	}
	w.Header().Set("Content-Type", "application/json")
	require.NoError(k.t, json.NewEncoder(w).Encode(resp))
}

// list lists the objects in bucket as ListObjectsV2 does
func (k *kamplexfsFake) list(w http.ResponseWriter, bucket, prefix, delimiter string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var names, dirs []string
	for name := range k.objects {
		key, ok := strings.CutPrefix(name, bucket+"/")
		if !ok || !strings.HasPrefix(key, prefix) {
			continue
		}
		if i := strings.Index(key[len(prefix):], delimiter); delimiter != "" && i >= 0 {
			dir := key[:len(prefix)+i+len(delimiter)]
			if !slices.Contains(dirs, dir) {
				dirs = append(dirs, dir)
			}
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)
	slices.Sort(dirs)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`,
		html.EscapeString(bucket), html.EscapeString(prefix), len(names)+len(dirs))
	for _, name := range names {
		o := k.objects[name]
		_, _ = fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>%s</LastModified><ETag>"%s"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`,
			html.EscapeString(strings.TrimPrefix(name, bucket+"/")), o.lastModified.UTC().Format("2006-01-02T15:04:05.000Z"), o.etag, o.size)
	}
	for _, dir := range dirs {
		_, _ = fmt.Fprintf(w, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, html.EscapeString(dir))
	}
	_, _ = io.WriteString(w, `</ListBucketResult>`)
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

		// mtime metadata from an upload is preferred when known, at
		// the precision of the listings
		o.meta = map[string]string{metaMtime: "1709608273.5"}
		assert.Equal(t, time.Unix(1709608273, 0), o.ModTime(ctx))
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

// TestKamPlexFSFingerprint checks an object has the same fingerprint
// whether it comes from an upload, a move, a copy, a HEAD or a
// listing, as the VFS cache drops a file whose fingerprint changes.
func TestKamPlexFSFingerprint(t *testing.T) {
	ctx := context.Background()
	mtime := time.Unix(1709608272, 123456789)
	for _, test := range []struct {
		name  string
		caps  string
		extra configmap.Simple
		want  time.Time
	}{
		{"MtimeNanos", kamplexfsAllFeatures, nil, time.Unix(1709608272, 0)},
		{"OldServer", `{"features":["rename-object"]}`, nil, time.Unix(1709608272, 0)},
		{"Exact", kamplexfsAllFeatures, configmap.Simple{"kamplexfs_exact_modtime": "true"}, mtime},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			k.capsBody = test.caps
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, test.extra)
			require.True(t, f.opt.NoHead, "uploads must build the object without a HEAD")
			put := func(remote string) fs.Object {
				src := object.NewStaticObjectInfo(remote, mtime, 5, true, nil, nil)
				o, err := f.Put(ctx, strings.NewReader("hello"), src)
				require.NoError(t, err)
				return o
			}

			uploaded := put("uploaded.txt")
			moved, err := f.Features().Move(ctx, put("src.txt"), "moved.txt")
			require.NoError(t, err)
			copied, err := f.Features().Copy(ctx, uploaded, "copied.txt")
			require.NoError(t, err)
			headed, err := f.NewObject(ctx, "uploaded.txt")
			require.NoError(t, err)

			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			listed := map[string]fs.Object{}
			for _, entry := range entries {
				listed[entry.Remote()] = entry.(fs.Object)
			}
			assert.Len(t, listed, 3)
			for _, o := range []fs.Object{uploaded, moved, copied, headed} {
				l := listed[o.Remote()]
				require.NotNil(t, l, o.Remote())
				assert.True(t, test.want.Equal(o.ModTime(ctx)), "%s: got %v want %v", o.Remote(), o.ModTime(ctx), test.want)
				assert.Equal(t, fs.Fingerprint(ctx, l, false), fs.Fingerprint(ctx, o, false), o.Remote())
				assert.Equal(t, fs.Fingerprint(ctx, l, true), fs.Fingerprint(ctx, o, true), o.Remote())
			}
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
