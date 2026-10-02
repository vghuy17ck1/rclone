package kamplexfs

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/kamplexfs/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hashNames returns the hashes of a server with BLAKE3 and, if md5
// is set, MD5.
func hashNames(md5 bool) []string {
	if md5 {
		return []string{"md5", "blake3"}
	}
	return []string{"blake3"}
}

// newBlake3Server makes a fake server with BLAKE3 and, if md5 is
// set, MD5.
func newBlake3Server(md5 bool) *fakeServer {
	fake := newFakeServer(testSecret)
	fake.hashes = hashNames(md5)
	fake.blake3 = true
	fake.noMD5 = !md5
	return fake
}

// newFsAt makes another Fs with root talking to the server at url
func newFsAt(ctx context.Context, t *testing.T, url, root string) (fs.Fs, error) {
	regInfo, err := fs.Find("kamplexfs")
	require.NoError(t, err)
	cfg := configmap.Simple{
		"url":        url,
		"jwt_secret": obscure.MustObscure(testSecret),
	}
	return NewFs(ctx, "TestKamPlexFSAgain", root, fs.ConfigMap("kamplexfs", regInfo.Options, "TestKamPlexFSAgain", cfg))
}

// blake3Of returns the BLAKE3 of s
func blake3Of(s string) string {
	mh, _ := hash.NewMultiHasherTypes(hash.NewHashSet(hash.BLAKE3))
	_, _ = mh.Write([]byte(s))
	return mh.Sums()[hash.BLAKE3]
}

func TestBlake3Hashes(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		hashes    []string
		bucketAPI bool
		want      hash.Set
	}{
		{"OldServer", nil, true, hash.Set(hash.MD5)},
		{"NoBucketAPI", nil, false, hash.Set(hash.MD5)},
		{"NoBLAKE3", []string{"md5"}, true, hash.Set(hash.MD5)},
		{"NoHashes", []string{}, true, hash.Set(hash.None)},
		{"MD5AndBLAKE3", hashNames(true), true, hash.NewHashSet(hash.MD5, hash.BLAKE3)},
		{"BLAKE3Only", hashNames(false), true, hash.Set(hash.BLAKE3)},
		{"BLAKE3OnlyNoBucketAPI", hashNames(false), false, hash.Set(hash.BLAKE3)},
		{"OtherHashes", []string{"md5", "sha1", "sha3"}, true, hash.Set(hash.MD5)},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeServer(testSecret)
			fake.hashes = test.hashes
			fake.bucketAPI = test.bucketAPI
			f, srv := newTestFs(ctx, t, fake, nil)
			assert.Empty(t, fake.Requests(), "NewFs must not ask")
			assert.Equal(t, test.want, f.Hashes())
			assert.Equal(t, test.want&^hash.Set(hash.BLAKE3), f.FingerprintHashes())
			assert.NotEmpty(t, fake.Requests())

			// The server is only asked once
			fake.Reset()
			assert.Equal(t, test.want, f.Hashes())
			g, err := newFsAt(ctx, t, srv.URL, "bucket")
			require.NoError(t, err)
			assert.Equal(t, test.want, g.Hashes())
			assert.Empty(t, fake.Requests())
		})
	}
}

func TestBlake3HashesRefused(t *testing.T) {
	ctx := context.Background()
	fake := newBlake3Server(true)
	f, srv := newTestFs(ctx, t, fake, nil)
	fake.FailNext(1, http.StatusForbidden, "Token does not permit this resource")
	assert.Equal(t, hash.Set(hash.MD5), f.Hashes())
	// This Fs doesn't ask again
	assert.Equal(t, hash.Set(hash.MD5), f.Hashes())
	assert.Len(t, fake.Requests(), 1)

	// but another one does
	g, err := newFsAt(ctx, t, srv.URL, "bucket")
	require.NoError(t, err)
	assert.Equal(t, hash.NewHashSet(hash.MD5, hash.BLAKE3), g.Hashes())
	requests := fake.Requests()
	require.Len(t, requests, 2)
	assert.Equal(t, "GET", requests[1].Method)
	assert.Equal(t, "/api/v1/buckets", requests[1].Path)

	// Any listing of the buckets updates the hashes
	fake.hashes = nil
	h, err := newFsAt(ctx, t, srv.URL, "")
	require.NoError(t, err)
	_, err = h.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, hash.Set(hash.MD5), g.Hashes())
}

func TestBlake3OldServer(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	// A digest from a server without the capability is ignored
	fake.blake3 = true
	fake.blake3Wrong = true
	f, _ := newTestFs(ctx, t, fake, nil)
	o := put(ctx, t, f, "file.txt", "hello", time.Now())
	_, err := o.Hash(ctx, hash.BLAKE3)
	assert.ErrorIs(t, err, hash.ErrUnsupported)
}

func TestBlake3Listing(t *testing.T) {
	ctx := context.Background()
	fake := newBlake3Server(false)
	f, _ := newTestFs(ctx, t, fake, nil)
	put(ctx, t, f, "dir/a.txt", "a", time.Now())
	put(ctx, t, f, "dir/b.txt", "b", time.Now())
	fake.mu.Lock()
	fake.files["bucket/dir/b.txt"].blake3Pending = true
	fake.mu.Unlock()

	entries, err := f.List(ctx, "dir")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	fake.Reset()
	for _, entry := range entries {
		digest, err := entry.(fs.Object).Hash(ctx, hash.BLAKE3)
		require.NoError(t, err)
		if entry.Remote() == "dir/a.txt" {
			assert.Equal(t, blake3Of("a"), digest)
		} else {
			assert.Empty(t, digest, "pending")
		}
		md5, err := entry.(fs.Object).Hash(ctx, hash.MD5)
		require.NoError(t, err)
		assert.Empty(t, md5)
	}
	assert.Empty(t, fake.Requests(), "the listing has the BLAKE3")
}

func TestBlake3Validation(t *testing.T) {
	for _, test := range []struct {
		in   *string
		want string
	}{
		{nil, ""},
		{new(""), ""},
		{new(strings.Repeat("a", 64)), strings.Repeat("a", 64)},
		{new(strings.Repeat("A", 64)), ""},
		{new(strings.Repeat("a", 63)), ""},
		{new("PENDING"), ""},
	} {
		assert.Equal(t, test.want, (&api.File{Blake3Checksum: test.in}).Blake3())
	}
}

func TestBlake3Upload(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		md5     bool
		wrong   bool
		pending bool
		wantErr bool
	}{
		{name: "OK", md5: true},
		{name: "BLAKE3Only"},
		{name: "Mismatch", md5: true, wrong: true, wantErr: true},
		{name: "MismatchBLAKE3Only", wrong: true, wantErr: true},
		{name: "Pending", wrong: true, pending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newBlake3Server(test.md5)
			f, _ := newTestFs(ctx, t, fake, nil)
			fake.blake3Wrong = test.wrong
			// The server computes it in the background
			fake.blake3 = !test.pending
			o, err := f.Put(ctx, strings.NewReader("hello"), object.NewStaticObjectInfo("file.txt", time.Now(), 5, true, nil, nil))
			if test.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "BLAKE3 differ")
				return
			}
			require.NoError(t, err)
			digest, err := o.Hash(ctx, hash.BLAKE3)
			require.NoError(t, err)
			switch {
			case test.pending:
				assert.Empty(t, digest)
			default:
				assert.Equal(t, blake3Of("hello"), digest)
			}
		})
	}
}

func TestBlake3Fingerprint(t *testing.T) {
	ctx := context.Background()
	for _, md5 := range []bool{true, false} {
		t.Run(map[bool]string{true: "MD5AndBLAKE3", false: "BLAKE3Only"}[md5], func(t *testing.T) {
			fake := newBlake3Server(md5)
			f, _ := newTestFs(ctx, t, fake, nil)
			if !md5 {
				// Without the FingerprintHasher, fs.Fingerprint
				// would use the BLAKE3
				require.Equal(t, hash.BLAKE3, f.Hashes().GetOne())
			}
			// The server computes it after the upload
			fake.blake3 = false
			uploaded := put(ctx, t, f, "file.txt", "hello", time.Unix(1709608272, 0))
			fake.blake3 = true

			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			require.Len(t, entries, 1)
			listed := entries[0].(fs.Object)
			digest, err := listed.Hash(ctx, hash.BLAKE3)
			require.NoError(t, err)
			require.Equal(t, blake3Of("hello"), digest)
			for _, fast := range []bool{true, false} {
				fingerprint := fs.Fingerprint(ctx, listed, fast)
				assert.Equal(t, fs.Fingerprint(ctx, uploaded, fast), fingerprint)
				assert.NotContains(t, fingerprint, digest)
				if md5 {
					assert.Contains(t, fingerprint, "5d41402abc4b2a76b9719d911017c592")
				}
			}
		})
	}
}
