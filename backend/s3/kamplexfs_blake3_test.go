package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// kamplexfsHashFeatures returns the capabilities of a server with
// BLAKE3 and, if md5 is set, MD5.
func kamplexfsHashFeatures(md5 bool) string {
	features := `"rename-object","rename-prefix","sequential-multipart","mtime-nanos","checksums","blake3"`
	if md5 {
		features += `,"md5"`
	}
	return `{"version":"1.3.0","maxKeys":2500,"features":[` + features + `]}`
}

// newKamPlexFSBlake3Fake makes a fake server with BLAKE3 and, if md5
// is set, MD5.
func newKamPlexFSBlake3Fake(t *testing.T, md5 bool) *kamplexfsFake {
	k := newKamPlexFSFake(t, false)
	k.capsBody = kamplexfsHashFeatures(md5)
	k.blake3 = true
	k.noMD5 = !md5
	return k
}

// putData uploads data as remote
func putData(ctx context.Context, f fs.Fs, remote string, data []byte) (fs.Object, error) {
	src := object.NewStaticObjectInfo(remote, time.Unix(1709608272, 0), int64(len(data)), true, nil, nil)
	return f.Put(ctx, bytes.NewReader(data), src)
}

func TestKamPlexFSBlake3Hashes(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name string
		caps string
		want hash.Set
	}{
		{"OldServer", kamplexfsAllFeatures, hash.Set(hash.MD5)},
		{"MD5AndBLAKE3", kamplexfsHashFeatures(true), hash.NewHashSet(hash.MD5, hash.BLAKE3)},
		{"BLAKE3Only", kamplexfsHashFeatures(false), hash.Set(hash.BLAKE3)},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			k.capsBody = test.caps
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
			assert.Equal(t, test.want, f.Hashes())
			assert.Equal(t, test.want&^hash.Set(hash.BLAKE3), f.FingerprintHashes())
		})
	}
}

func TestKamPlexFSBlake3OldServer(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSFake(t, false)
	// Headers from a server which didn't advertise BLAKE3 are ignored
	k.blake3 = true
	k.blake3Wrong = true
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)

	o, err := putData(ctx, f, "file.txt", []byte("hello"))
	require.NoError(t, err)
	_, err = o.Hash(ctx, hash.BLAKE3)
	assert.ErrorIs(t, err, hash.ErrUnsupported)
	md5, err := o.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", md5)
	assert.Empty(t, o.(*Object).blake3)
}

func TestKamPlexFSBlake3Fingerprint(t *testing.T) {
	ctx := context.Background()
	for _, md5 := range []bool{true, false} {
		t.Run(fmt.Sprintf("MD5=%v", md5), func(t *testing.T) {
			k := newKamPlexFSBlake3Fake(t, md5)
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
			if !md5 {
				// Without the FingerprintHasher, fs.Fingerprint
				// would use the BLAKE3
				require.Equal(t, hash.BLAKE3, f.Hashes().GetOne())
			}

			data := []byte("hello")
			uploaded, err := putData(ctx, f, "file.txt", data)
			require.NoError(t, err)
			digest, err := uploaded.Hash(ctx, hash.BLAKE3)
			require.NoError(t, err)
			require.Equal(t, blake3Hex(data), digest)

			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			require.Len(t, entries, 1)
			listed := entries[0].(fs.Object)
			k.reset()
			for _, fast := range []bool{true, false} {
				fingerprint := fs.Fingerprint(ctx, listed, fast)
				assert.Equal(t, fs.Fingerprint(ctx, uploaded, fast), fingerprint)
				assert.NotContains(t, fingerprint, digest)
				if md5 {
					assert.Contains(t, fingerprint, "5d41402abc4b2a76b9719d911017c592")
				}
			}
			assert.Empty(t, k.requests, "fingerprints must not need a request")
		})
	}
}

func TestKamPlexFSBlake3Lazy(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSBlake3Fake(t, true)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{"list_chunk": "2"})
	files := []string{"dir/1.txt", "dir/2.txt", "dir/3.txt", "dir/4.txt", "dir/5.txt", "dir/6.txt", "dir/sub/7.txt", "dir/sub/8.txt", "other.txt"}
	for _, remote := range files {
		_, err := putData(ctx, f, remote, []byte(remote))
		require.NoError(t, err)
	}
	k.reset()

	listed := map[string]fs.Object{}
	for _, dir := range []string{"dir", "dir/sub"} {
		entries, err := f.List(ctx, dir)
		require.NoError(t, err)
		for _, entry := range entries {
			if o, ok := entry.(fs.Object); ok {
				listed[o.Remote()] = o
			}
		}
	}
	require.Len(t, listed, 8)
	assert.Empty(t, k.find("kamplexfsHashes"), "listings must not fetch BLAKE3")
	assert.Equal(t, 0, k.count("HEAD"))

	// After the listing, dir/3.txt changes size, dir/4.txt changes
	// data and dir/5.txt changes mtime
	_, err := putData(ctx, f, "dir/3.txt", []byte("changed dir/3.txt"))
	require.NoError(t, err)
	_, err = putData(ctx, f, "dir/4.txt", []byte("dir/X.txt"))
	require.NoError(t, err)
	k.mu.Lock()
	k.objects["bucket/dir/5.txt"].lastModified = time.Unix(1709608273, 0)
	k.mu.Unlock()
	k.reset()

	hashOf := func(remote, want string) {
		digest, err := listed[remote].Hash(ctx, hash.BLAKE3)
		require.NoError(t, err)
		assert.Equal(t, blake3Hex([]byte(want)), digest, remote)
	}

	// A single object is read with a HEAD request
	hashOf("dir/1.txt", "dir/1.txt")
	assert.Equal(t, 1, k.count("HEAD"))
	assert.Empty(t, k.find("kamplexfsHashes"))

	// The next one bulk lists the directory, in pages of list_chunk
	hashOf("dir/2.txt", "dir/2.txt")
	bulk := k.find("kamplexfsHashes")
	require.Len(t, bulk, 3)
	assert.Equal(t, "/s3/bucket", bulk[0].Path)
	assert.Contains(t, bulk[0].Query, "prefix=dir%2F")
	assert.Contains(t, bulk[0].Query, "delimiter=%2F")
	assert.Contains(t, bulk[0].Query, "max-keys=2")
	assert.NotContains(t, bulk[0].Query, "continuation-token")
	assert.Contains(t, bulk[1].Query, "continuation-token=dir%2F2.txt")
	assert.True(t, strings.HasPrefix(bulk[0].Header.Get("Authorization"), "AWS4-HMAC-SHA256 "), "must be SigV4 signed")

	// The rest of the directory comes from the listing
	k.reset()
	hashOf("dir/6.txt", "dir/6.txt")
	assert.Empty(t, k.requests)

	// unless it changed since it was listed
	hashOf("dir/3.txt", "changed dir/3.txt")
	hashOf("dir/4.txt", "dir/X.txt")
	hashOf("dir/5.txt", "dir/5.txt")
	assert.Equal(t, 3, k.count("HEAD"))

	// A subdirectory is listed on its own
	k.reset()
	hashOf("dir/sub/7.txt", "dir/sub/7.txt")
	hashOf("dir/sub/8.txt", "dir/sub/8.txt")
	assert.Equal(t, 1, k.count("HEAD"))
	bulk = k.find("kamplexfsHashes")
	require.Len(t, bulk, 1)
	assert.Contains(t, bulk[0].Query, "prefix=dir%2Fsub%2F")

	// Objects from NewObject have it from the HEAD
	k.reset()
	o, err := f.NewObject(ctx, "dir/1.txt")
	require.NoError(t, err)
	assert.Equal(t, 1, k.count("HEAD"))
	digest, err := o.Hash(ctx, hash.BLAKE3)
	require.NoError(t, err)
	assert.Equal(t, blake3Hex([]byte("dir/1.txt")), digest)
	assert.Equal(t, 1, k.count("HEAD"), "NewObject must have read the BLAKE3")
}

func TestKamPlexFSBlake3CacheExpiry(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSBlake3Fake(t, true)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	now := time.Now()
	f.kpx.blake3.now = func() time.Time { return now }
	for _, remote := range []string{"1.txt", "2.txt", "3.txt"} {
		_, err := putData(ctx, f, remote, []byte(remote))
		require.NoError(t, err)
	}
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	k.reset()
	for _, entry := range entries[:2] {
		_, err := entry.(fs.Object).Hash(ctx, hash.BLAKE3)
		require.NoError(t, err)
	}
	require.Len(t, k.find("kamplexfsHashes"), 1)

	// After the TTL the digests aren't used
	now = now.Add(kamplexfsBlake3TTL + time.Second)
	_, err = entries[2].(fs.Object).Hash(ctx, hash.BLAKE3)
	require.NoError(t, err)
	assert.Equal(t, 2, k.count("HEAD"))
	assert.Len(t, k.find("kamplexfsHashes"), 1)
}

func TestKamPlexFSBlake3Upload(t *testing.T) {
	ctx := context.Background()
	big := bytes.Repeat([]byte("0123456789abcdef"), 6*1024*1024/16)
	multipart := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M"}
	for _, test := range []struct {
		name    string
		md5     bool
		pending bool
		wrong   bool
		extra   configmap.Simple
		data    []byte
		wantErr bool
	}{
		{name: "OK", md5: true, data: []byte("hello")},
		{name: "BLAKE3Only", data: []byte("hello")},
		{name: "Mismatch", md5: true, wrong: true, data: []byte("hello"), wantErr: true},
		{name: "MismatchBLAKE3Only", wrong: true, data: []byte("hello"), wantErr: true},
		{name: "Pending", md5: true, pending: true, wrong: true, data: []byte("hello")},
		{name: "DisableChecksum", md5: true, wrong: true, extra: configmap.Simple{"disable_checksum": "true"}, data: []byte("hello")},
		{name: "Multipart", md5: true, extra: multipart, data: big},
		{name: "MultipartMismatch", wrong: true, extra: multipart, data: big, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSBlake3Fake(t, test.md5)
			k.blake3Pending = test.pending
			k.blake3Wrong = test.wrong
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, test.extra)
			o, err := putData(ctx, f, "file.bin", test.data)
			if test.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "BLAKE3 differ")
				return
			}
			require.NoError(t, err)
			if test.extra["upload_cutoff"] != "" {
				require.NotNil(t, k.find("uploadId"), "must be a multipart upload")
			}
			k.reset()
			digest, err := o.Hash(ctx, hash.BLAKE3)
			require.NoError(t, err)
			switch {
			case test.pending:
				assert.Empty(t, digest)
			case test.wrong:
				assert.Equal(t, strings.Repeat("0", 64), digest)
			default:
				assert.Equal(t, blake3Hex(test.data), digest)
			}
			assert.Empty(t, k.requests, "the upload must have set the BLAKE3")
			md5, err := o.Hash(ctx, hash.MD5)
			require.NoError(t, err)
			if test.md5 && test.extra["upload_cutoff"] == "" {
				assert.Len(t, md5, 32)
			}
			if !test.md5 {
				k.mu.Lock()
				etag := k.objects["bucket/file.bin"].etag
				k.mu.Unlock()
				assert.True(t, strings.HasPrefix(etag, "b3-"), etag)
			}
		})
	}
}

// TestKamPlexFSBlake3Sync syncs from a local directory to a server
// which has no MD5s.
func TestKamPlexFSBlake3Sync(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSBlake3Fake(t, false)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	dir := t.TempDir()
	mtime := time.Unix(1709608272, 0)
	for _, remote := range []string{"1.txt", "2.txt", "sub/3.txt", "sub/4.txt"} {
		path := filepath.Join(dir, filepath.FromSlash(remote))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o777))
		require.NoError(t, os.WriteFile(path, []byte(remote), 0o666))
		require.NoError(t, os.Chtimes(path, mtime, mtime))
	}
	local, err := fs.NewFs(ctx, dir)
	require.NoError(t, err)
	hashType, _ := operations.CommonHash(ctx, f, local)
	require.Equal(t, hash.BLAKE3, hashType)

	// The uploads are checked with the BLAKE3
	require.NoError(t, sync.Sync(ctx, f, local, false))
	assert.Len(t, k.objects, 4)

	// A plain sync doesn't need the BLAKE3
	k.reset()
	accounting.GlobalStats().ResetCounters()
	require.NoError(t, sync.Sync(ctx, f, local, false))
	assert.Equal(t, int64(0), accounting.GlobalStats().GetTransfers())
	assert.Empty(t, k.find("kamplexfsHashes"))
	assert.Equal(t, 0, k.count("HEAD"))

	// sync --checksum reads it from the bulk listing
	k.reset()
	ctx, ci := fs.AddConfig(ctx)
	ci.CheckSum = true
	require.NoError(t, sync.Sync(ctx, f, local, false))
	assert.Equal(t, int64(0), accounting.GlobalStats().GetTransfers())
	assert.NotEmpty(t, k.find("kamplexfsHashes"))
	assert.LessOrEqual(t, k.count("HEAD"), 2)
	assert.Equal(t, 0, k.count("PUT"))
}

func TestKamPlexFSBlake3Matches(t *testing.T) {
	listed := time.Unix(1709608272, 0)
	o := &Object{bytes: 5, lastModified: listed, md5: "5d41402abc4b2a76b9719d911017c592"}
	noMD5 := &Object{bytes: 5, lastModified: listed}
	for _, test := range []struct {
		name string
		o    *Object
		d    kamplexfsBlake3Digest
		want bool
	}{
		{"Same", o, kamplexfsBlake3Digest{size: 5, etag: o.md5, lastModified: listed}, true},
		{"NoETagOrTime", o, kamplexfsBlake3Digest{size: 5}, true},
		{"Size", o, kamplexfsBlake3Digest{size: 6, etag: o.md5, lastModified: listed}, false},
		{"SizeNoMD5", noMD5, kamplexfsBlake3Digest{size: 6, etag: "b3-x", lastModified: listed}, false},
		{"ETag", o, kamplexfsBlake3Digest{size: 5, etag: "00000000000000000000000000000000", lastModified: listed}, false},
		{"ETagNoMD5", noMD5, kamplexfsBlake3Digest{size: 5, etag: "b3-x", lastModified: listed}, true},
		{"LastModified", o, kamplexfsBlake3Digest{size: 5, etag: o.md5, lastModified: listed.Add(time.Second)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, test.d.matches(test.o))
		})
	}
}

func TestKamPlexFSBlake3Header(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSBlake3Fake(t, true)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
	digest := strings.Repeat("a", 64)
	for _, test := range []struct {
		header string
		want   string
	}{
		{"", ""},
		{digest, digest},
		{strings.ToUpper(digest), ""},
		{digest[:63], ""},
		{"PENDING", ""},
	} {
		o := &Object{fs: f, blake3: "old"}
		o.kamplexfsSetBlake3FromHeader(http.Header{kamplexfsBlake3Header: {test.header}})
		assert.Equal(t, test.want, o.blake3, test.header)
	}
}

func TestKamPlexFSBlake3HashesOption(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		caps   string
		option string
		want   hash.Set
	}{
		{"BLAKE3Off", kamplexfsHashFeatures(true), "md5", hash.Set(hash.MD5)},
		{"BLAKE3Only", kamplexfsHashFeatures(true), "blake3", hash.Set(hash.BLAKE3)},
		{"Both", kamplexfsAllFeatures, "md5, blake3", hash.NewHashSet(hash.MD5, hash.BLAKE3)},
		{"None", kamplexfsHashFeatures(true), "none", hash.Set(hash.None)},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSFake(t, false)
			k.capsBody = test.caps
			k.blake3 = true
			k.blake3Wrong = true
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{"hashes": test.option})
			assert.Equal(t, test.want, f.Hashes())

			o, err := putData(ctx, f, "file.txt", []byte("hello"))
			if test.want.Contains(hash.BLAKE3) {
				// The upload is checked
				require.Error(t, err)
				assert.Contains(t, err.Error(), "BLAKE3 differ")
				return
			}
			require.NoError(t, err)
			_, err = o.Hash(ctx, hash.BLAKE3)
			assert.ErrorIs(t, err, hash.ErrUnsupported)
		})
	}

	t.Run("Invalid", func(t *testing.T) {
		k := newKamPlexFSFake(t, false)
		regInfo, err := fs.Find("s3")
		require.NoError(t, err)
		m := fs.ConfigMap("s3", regInfo.Options, "TestKamPlexFS", configmap.Simple{
			"provider": kamplexfsProvider,
			"endpoint": k.endpoint(),
			"hashes":   "md5,sha1",
		})
		_, err = NewFs(ctx, "TestKamPlexFS", "bucket", m)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"sha1" isn't md5, blake3 or none`)
	})
}
