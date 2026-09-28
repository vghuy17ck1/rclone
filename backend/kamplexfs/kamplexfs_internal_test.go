package kamplexfs

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rclone/rclone/backend/kamplexfs/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSecret = "test secret"

// tickingClock returns a clock starting now which advances by a
// second each time it is read, so every minted token is different.
func tickingClock() func() time.Time {
	var mu sync.Mutex
	t := time.Now()
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(time.Second)
		return t
	}
}

// newTestFs makes an Fs with root "bucket" talking to fake
func newTestFs(ctx context.Context, t *testing.T, fake *fakeServer, extra configmap.Simple) (*Fs, *httptest.Server) {
	fake.buckets["bucket"] = true
	return newTestFsRoot(ctx, t, fake, "bucket", extra)
}

// newTestFsRoot makes an Fs with root talking to fake
func newTestFsRoot(ctx context.Context, t *testing.T, fake *fakeServer, root string, extra configmap.Simple) (*Fs, *httptest.Server) {
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	regInfo, err := fs.Find("kamplexfs")
	require.NoError(t, err)
	cfg := configmap.Simple{
		"url":        srv.URL,
		"jwt_secret": obscure.MustObscure(testSecret),
	}
	for k, v := range extra {
		cfg[k] = v
	}
	f, err := NewFs(ctx, "TestKamPlexFSInternal", root, fs.ConfigMap("kamplexfs", regInfo.Options, "TestKamPlexFSInternal", cfg))
	require.NoError(t, err)
	f.(*Fs).tokens.now = tickingClock()
	fake.Reset()
	return f.(*Fs), srv
}

// put uploads a file with contents at remote
func put(ctx context.Context, t *testing.T, f *Fs, remote, contents string, modTime time.Time) fs.Object {
	src := object.NewStaticObjectInfo(remote, modTime, int64(len(contents)), true, nil, nil)
	o, err := f.Put(ctx, bytes.NewBufferString(contents), src)
	require.NoError(t, err)
	return o
}

// parseClaims decodes a token signed with testSecret
func parseClaims(t *testing.T, token string) jwt.MapClaims {
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(token, claims, func(*jwt.Token) (any, error) {
		return []byte(testSecret), nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithoutClaimsValidation())
	require.NoError(t, err)
	return claims
}

func TestTokenClaims(t *testing.T) {
	now := time.Unix(1782012030, 0)
	ts := &tokenSource{secret: []byte(testSecret), ttl: time.Hour, now: func() time.Time { return now }}
	token, err := ts.Token()
	require.NoError(t, err)
	claims := parseClaims(t, token)
	assert.Equal(t, float64(now.Add(-30*time.Second).Unix()), claims["iat"])
	assert.Equal(t, float64(now.Add(time.Hour).Unix()), claims["exp"])
	assert.NotContains(t, claims, "allowed_prefixes")
	assert.NotContains(t, claims, "allowed_methods")

	ts = &tokenSource{secret: []byte(testSecret), ttl: time.Hour, now: time.Now,
		prefixes: []string{"bucket/a/"}, methods: []string{"GET", "PUT"}}
	token, err = ts.Token()
	require.NoError(t, err)
	claims = parseClaims(t, token)
	assert.Equal(t, []any{"bucket/a/"}, claims["allowed_prefixes"])
	assert.Equal(t, []any{"GET", "PUT"}, claims["allowed_methods"])
}

func TestTokenRefreshInsideMargin(t *testing.T) {
	now := time.Unix(1782012030, 0)
	ts := &tokenSource{secret: []byte(testSecret), ttl: time.Hour, now: func() time.Time { return now }}
	first, err := ts.Token()
	require.NoError(t, err)
	// margin is max(60s, ttl/10) = 6m
	now = now.Add(time.Hour - 6*time.Minute - time.Second)
	again, err := ts.Token()
	require.NoError(t, err)
	assert.Equal(t, first, again, "must reuse a token outside the margin")
	now = now.Add(2 * time.Second)
	refreshed, err := ts.Token()
	require.NoError(t, err)
	assert.NotEqual(t, first, refreshed, "must refresh a token inside the margin")

	// the margin is at least 60s
	ts = &tokenSource{secret: []byte(testSecret), ttl: 5 * time.Minute, now: func() time.Time { return now }}
	first, err = ts.Token()
	require.NoError(t, err)
	now = now.Add(5*time.Minute - 59*time.Second)
	refreshed, err = ts.Token()
	require.NoError(t, err)
	assert.NotEqual(t, first, refreshed)
}

func TestTokenConcurrentMint(t *testing.T) {
	ts := &tokenSource{secret: []byte(testSecret), ttl: time.Hour, now: tickingClock()}
	stale, err := ts.Token()
	require.NoError(t, err)

	var wg sync.WaitGroup
	var mu sync.Mutex
	tokens := map[string]bool{}
	for range 50 {
		wg.Go(func() {
			// every request which saw the stale token rejected
			// invalidates it, then asks for a new one
			ts.Invalidate(stale)
			token, err := ts.Token()
			assert.NoError(t, err)
			mu.Lock()
			tokens[token] = true
			mu.Unlock()
		})
	}
	wg.Wait()
	assert.Len(t, tokens, 1, "concurrent requests must share one new token")
	assert.NotContains(t, tokens, stale)
}

func TestAuthorization(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)

	t.Run("HeaderNotURL", func(t *testing.T) {
		fake.Reset()
		_, err := f.List(ctx, "")
		require.NoError(t, err)
		reqs := fake.Requests()
		require.Len(t, reqs, 1)
		assert.True(t, strings.HasPrefix(reqs[0].Auth, "Bearer "))
		assert.NotContains(t, reqs[0].Query, "jwt")
	})

	t.Run("401RefreshesAndRetriesOnce", func(t *testing.T) {
		fake.Reset()
		fake.FailNext(1, http.StatusUnauthorized, "Token expired")
		_, err := f.List(ctx, "")
		require.NoError(t, err)
		reqs := fake.Requests()
		require.Len(t, reqs, 2)
		assert.Equal(t, http.StatusUnauthorized, reqs[0].Status)
		assert.Equal(t, http.StatusOK, reqs[1].Status)
		assert.NotEqual(t, reqs[0].Auth, reqs[1].Auth, "must retry with a new token")
	})

	t.Run("Second401IsAnError", func(t *testing.T) {
		fake.Reset()
		fake.FailNext(10, http.StatusUnauthorized, "Invalid token")
		_, err := f.List(ctx, "")
		require.Error(t, err)
		assert.Equal(t, http.StatusUnauthorized, statusCode(err))
		assert.Contains(t, err.Error(), "Invalid token")
		assert.Len(t, fake.Requests(), 2, "one retry only")
		fake.FailNext(0, 0, "")
	})

	t.Run("403NotRetried", func(t *testing.T) {
		fake.Reset()
		fake.FailNext(10, http.StatusForbidden, "Token does not permit this resource")
		_, err := f.List(ctx, "")
		require.Error(t, err)
		assert.Equal(t, http.StatusForbidden, statusCode(err))
		assert.Contains(t, err.Error(), "Token does not permit this resource")
		assert.Len(t, fake.Requests(), 1)
		fake.FailNext(0, 0, "")
	})

	t.Run("ServerChecksTheToken", func(t *testing.T) {
		fake.maxTTL = time.Hour
		defer func() { fake.maxTTL = 24 * time.Hour }()
		f, _ := newTestFs(ctx, t, fake, configmap.Simple{"jwt_ttl": "2h"})
		_, err := f.List(ctx, "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Token exp exceeds maximum")
	})

	t.Run("ScopeClaimsDeny", func(t *testing.T) {
		f, _ := newTestFs(ctx, t, fake, configmap.Simple{"allowed_prefixes": "bucket/allowed/"})
		_, err := f.List(ctx, "denied")
		assert.Equal(t, http.StatusForbidden, statusCode(err))
	})

	t.Run("StaticTokenNotRefreshed", func(t *testing.T) {
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"exp": time.Now().Add(time.Hour).Unix(),
		}).SignedString([]byte(testSecret))
		require.NoError(t, err)
		f, _ := newTestFs(ctx, t, fake, configmap.Simple{"jwt_secret": "", "token": token})
		_, err = f.List(ctx, "")
		require.NoError(t, err)
		fake.Reset()
		fake.FailNext(10, http.StatusUnauthorized, "Token expired")
		_, err = f.List(ctx, "")
		assert.Equal(t, http.StatusUnauthorized, statusCode(err))
		assert.Len(t, fake.Requests(), 1)
		fake.FailNext(0, 0, "")
	})
}

func TestRoute(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	fake.route = "/custom/files"
	f, _ := newTestFs(ctx, t, fake, configmap.Simple{"route": "custom/files/"})
	put(ctx, t, f, "a.txt", "hello", time.Now())
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, entries, 1)
}

func TestHashPending(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)
	o := put(ctx, t, f, "a.txt", "hello", time.Now())
	md5, err := o.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", md5)

	fake.files["bucket/a.txt"].pending = true
	o, err = f.NewObject(ctx, "a.txt")
	require.NoError(t, err)
	md5, err = o.Hash(ctx, hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "", md5)

	assert.Equal(t, "", (&api.File{MD5Checksum: "temp-0123456789abcdef"}).MD5())
}

func TestUploadRequest(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)
	mtime := time.Unix(1709608272, 123456700)

	src := object.NewStaticObjectInfo("dir/a b+.txt", mtime, 5, true, map[hash.Type]string{hash.MD5: "5d41402abc4b2a76b9719d911017c592"}, nil)
	o, err := f.Put(ctx, bytes.NewBufferString("hello"), src)
	require.NoError(t, err)
	reqs := fake.Requests()
	require.Len(t, reqs, 2, "must not stat after the upload")
	// the bucket is made on the first upload only
	assert.Equal(t, "POST", reqs[0].Method)
	assert.Equal(t, "/api/v1/buckets", reqs[0].Path)
	reqs = reqs[1:]
	assert.Equal(t, "PUT", reqs[0].Method)
	assert.Equal(t, "/content/bucket/dir/a b+.txt", reqs[0].Path)
	assert.Contains(t, reqs[0].Query, "md5=5d41402abc4b2a76b9719d911017c592")
	// nothing has shown the server stores sub-second mtimes yet
	assert.True(t, strings.HasSuffix(reqs[0].Query, "&mtime=1709608272"), reqs[0].Query)
	assert.Equal(t, time.Second, f.Precision())
	assert.Equal(t, int64(5), o.Size())

	// A listing with a fractional mtime shows the server stores them
	fake.files["bucket/dir/other.txt"] = &fakeFile{data: []byte("x"), mtime: time.Unix(1, 5)}
	_, err = f.List(ctx, "dir")
	require.NoError(t, err)
	fake.Reset()
	o = put(ctx, t, f, "dir/b.txt", "hello", mtime)
	require.Len(t, fake.Requests(), 1)
	assert.Contains(t, fake.Requests()[0].Query, "mtime=1709608272.1234567")
	assert.True(t, mtime.Equal(o.ModTime(ctx)))

	// A bad md5 is rejected by the server
	src = object.NewStaticObjectInfo("c.txt", mtime, 5, true, map[hash.Type]string{hash.MD5: "00000000000000000000000000000000"}, nil)
	_, err = f.Put(ctx, bytes.NewBufferString("hello"), src)
	assert.Equal(t, http.StatusBadRequest, statusCode(err))
}

func TestMissingSource(t *testing.T) {
	ctx := context.Background()
	// This server reports success when moving a missing source
	fake := newFakeServer(testSecret).oldServer()
	f, _ := newTestFs(ctx, t, fake, nil)
	o := put(ctx, t, f, "a.txt", "hello", time.Now())
	delete(fake.files, "bucket/a.txt")
	fake.Reset()

	_, err := f.Move(ctx, o, "b.txt")
	assert.Equal(t, fs.ErrorObjectNotFound, err)
	_, err = f.Copy(ctx, o, "b.txt")
	assert.Equal(t, fs.ErrorObjectNotFound, err)
	for _, r := range fake.Requests() {
		assert.NotEqual(t, "POST", r.Method, "must not move or copy a missing source")
	}

	err = f.DirMove(ctx, f, "missing", "dst")
	assert.Equal(t, fs.ErrorDirNotFound, err)

	// The source vanishes between the check and the move
	o = put(ctx, t, f, "a.txt", "hello", time.Now())
	fake.FailAfter(1, 1, http.StatusOK, `{"kind":"kamplexfs#folder","name":"","path":"","parentPath":""}`)
	_, err = f.Move(ctx, o, "b.txt")
	assert.Equal(t, fs.ErrorObjectNotFound, err)
}

func TestDirMove(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)
	put(ctx, t, f, "src/a.txt", "hello", time.Now())
	put(ctx, t, f, "src/sub/b.txt", "hello", time.Now())
	put(ctx, t, f, "exists/c.txt", "hello", time.Now())

	assert.Equal(t, fs.ErrorDirExists, f.DirMove(ctx, f, "src", "exists"))

	fake.Reset()
	require.NoError(t, f.DirMove(ctx, f, "src", "dst"))
	moves := 0
	for _, r := range fake.Requests() {
		if r.Method == "POST" && r.Path == "/move" {
			moves++
		}
	}
	assert.Equal(t, 1, moves, "must move the folder in one request")
	_, err := f.NewObject(ctx, "dst/sub/b.txt")
	assert.NoError(t, err)
}

func TestSetModTimeFallback(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)
	o := put(ctx, t, f, "a.txt", "hello", time.Now())
	mtime := time.Unix(1709608272, 0)
	require.NoError(t, o.SetModTime(ctx, mtime))
	assert.True(t, mtime.Equal(o.ModTime(ctx)))
	assert.True(t, mtime.Equal(fake.files["bucket/a.txt"].mtime))

	old := newFakeServer(testSecret).oldServer()
	f, _ = newTestFs(ctx, t, old, nil)
	o = put(ctx, t, f, "a.txt", "hello", time.Now())
	old.Reset()
	assert.Equal(t, fs.ErrorCantSetModTime, o.SetModTime(ctx, mtime))
	assert.Equal(t, fs.ErrorCantSetModTime, o.SetModTime(ctx, mtime))
	assert.Len(t, old.Requests(), 1, "must remember the server can't set mtimes")
}

// TestFingerprint checks a file has the same fingerprint whether it
// comes from an upload, a move, a copy, a read or a listing, as the
// VFS cache drops a file whose fingerprint changes.
func TestFingerprint(t *testing.T) {
	ctx := context.Background()
	mtime := time.Unix(1709608272, 123456789)
	for _, test := range []struct {
		name  string
		fake  *fakeServer
		nanos bool
		want  time.Time
	}{
		{"Nanos", newFakeServer(testSecret), true, mtime},
		{"OldServer", newFakeServer(testSecret).oldServer(), false, time.Unix(1709608272, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, _ := newTestFs(ctx, t, test.fake, nil)
			// as after a listing with sub-second mtimes
			f.server.mtimeNanos.Store(test.nanos)

			uploaded := put(ctx, t, f, "uploaded.txt", "hello", mtime)
			moved, err := f.Move(ctx, put(ctx, t, f, "src.txt", "hello", mtime), "moved.txt")
			require.NoError(t, err)
			copied, err := f.Copy(ctx, uploaded, "copied.txt")
			require.NoError(t, err)
			read, err := f.NewObject(ctx, "uploaded.txt")
			require.NoError(t, err)

			entries, err := f.List(ctx, "")
			require.NoError(t, err)
			listed := map[string]fs.Object{}
			for _, entry := range entries {
				listed[entry.Remote()] = entry.(fs.Object)
			}
			assert.Len(t, listed, 3)
			for _, o := range []fs.Object{uploaded, moved, copied, read} {
				l := listed[o.Remote()]
				require.NotNil(t, l, o.Remote())
				assert.True(t, test.want.Equal(o.ModTime(ctx)), "%s: got %v want %v", o.Remote(), o.ModTime(ctx), test.want)
				assert.Equal(t, fs.Fingerprint(ctx, l, false), fs.Fingerprint(ctx, o, false), o.Remote())
			}
		})
	}
}

func TestListR(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name      string
		recursive bool
	}{
		{"Recursive", true},
		{"NotRecursive", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeServer(testSecret)
			fake.packed = true
			fake.recursive = test.recursive
			f, _ := newTestFs(ctx, t, fake, configmap.Simple{"list_chunk": "2"})
			for _, p := range []string{"a.txt", "d/b.txt", "d/e/c.txt", "d/e/f/g.txt", "h/i.txt"} {
				put(ctx, t, f, p, "x", time.Now())
			}
			// empty folders can't exist here
			require.NoError(t, f.Mkdir(ctx, "empty"))

			fake.Reset()
			var got []string
			err := f.ListR(ctx, "", func(entries fs.DirEntries) error {
				for _, e := range entries {
					got = append(got, e.Remote())
				}
				return nil
			})
			require.NoError(t, err)
			assert.Subset(t, got, []string{"a.txt", "d/b.txt", "d/e/c.txt", "d/e/f/g.txt", "h/i.txt"})
			listPaths := map[string]bool{}
			for _, r := range fake.Requests() {
				listPaths[r.Path] = true
				if test.recursive {
					assert.Contains(t, r.Query, "recursive=true")
				}
			}
			if test.recursive {
				assert.Equal(t, map[string]bool{"/list/bucket": true}, listPaths, "must list the bucket in one paged scan")
				assert.Len(t, fake.Requests(), 3)
			} else {
				assert.Contains(t, listPaths, "/list/bucket/d/e/f/")
				// the pages of the first listing keep its parameters
				for _, r := range fake.Requests() {
					if strings.Contains(r.Query, "recursive=true") {
						assert.Equal(t, "/list/bucket", r.Path, "must stop asking for recursive listings")
					}
				}
			}
		})
	}

	// On direct-fs servers recursive listings are only used if they
	// have every folder, so empty folders aren't left out
	for _, test := range []struct {
		name   string
		marked bool
	}{
		{"DirectFSMarked", true},
		{"DirectFSNotMarked", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newFakeServer(testSecret)
			fake.recursiveFolders = test.marked
			f, _ := newTestFs(ctx, t, fake, configmap.Simple{"list_chunk": "2"})
			for _, p := range []string{"a.txt", "d/b.txt", "d/e/c.txt", "h/i.txt"} {
				put(ctx, t, f, p, "x", time.Now())
			}
			require.NoError(t, f.Mkdir(ctx, "empty/deeper"))
			for range 2 {
				fake.Reset()
				var got []string
				require.NoError(t, f.ListR(ctx, "", func(entries fs.DirEntries) error {
					for _, e := range entries {
						got = append(got, e.Remote())
					}
					return nil
				}))
				assert.ElementsMatch(t, []string{"a.txt", "d", "d/b.txt", "d/e", "d/e/c.txt", "empty", "empty/deeper", "h", "h/i.txt"}, got)
				recursive := 0
				for _, r := range fake.Requests() {
					if strings.Contains(r.Query, "recursive=true") {
						recursive++
					}
				}
				if test.marked {
					assert.Equal(t, len(fake.Requests()), recursive, "must only use recursive listings")
					assert.Len(t, fake.Requests(), 2)
				} else {
					assert.Greater(t, len(fake.Requests()), recursive, "must walk the folders")
				}
			}
			// A recursive listing which isn't marked is only tried once
			if !test.marked {
				for _, r := range fake.Requests() {
					assert.NotContains(t, r.Query, "recursive=true")
				}
			}
		})
	}
}

func TestNotFound(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFs(ctx, t, fake, nil)
	o := put(ctx, t, f, "a.txt", "x", time.Now())
	require.NoError(t, o.Remove(ctx))
	assert.Equal(t, fs.ErrorObjectNotFound, o.Remove(ctx))
	assert.Equal(t, fs.ErrorDirNotFound, f.Purge(ctx, "missing"))
	_, err := f.List(ctx, "missing")
	assert.Equal(t, fs.ErrorDirNotFound, err)
	_, err = f.NewObject(ctx, "missing.txt")
	assert.Equal(t, fs.ErrorObjectNotFound, err)
}

// requestPaths returns "METHOD path" for each request
func requestPaths(fake *fakeServer) (out []string) {
	for _, r := range fake.Requests() {
		out = append(out, r.Method+" "+r.Path)
	}
	return out
}

func TestBuckets(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret)
	f, _ := newTestFsRoot(ctx, t, fake, "", nil)

	// Mkdir makes the bucket once
	require.NoError(t, f.Mkdir(ctx, "new-bucket"))
	require.NoError(t, f.Mkdir(ctx, "new-bucket"))
	assert.Equal(t, []string{"POST /api/v1/buckets"}, requestPaths(fake))
	assert.True(t, fake.buckets["new-bucket"])

	// Making a folder in a new bucket makes the bucket first
	fake.Reset()
	require.NoError(t, f.Mkdir(ctx, "other-bucket/dir"))
	assert.Equal(t, []string{"POST /api/v1/buckets", "POST /folders"}, requestPaths(fake))

	// The server rejects bad names
	err := f.Mkdir(ctx, "Bad_Name")
	assert.Equal(t, http.StatusBadRequest, statusCode(err))

	// The root lists the buckets
	fake.Reset()
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, entries, 2)
	assert.Equal(t, []string{"GET /api/v1/buckets"}, requestPaths(fake))

	// Rmdir removes empty buckets only, although the server would
	// delete one holding only empty folders
	assert.Equal(t, fs.ErrorDirectoryNotEmpty, f.Rmdir(ctx, "other-bucket"))
	assert.True(t, fake.buckets["other-bucket"])
	require.NoError(t, f.Rmdir(ctx, "new-bucket"))
	assert.False(t, fake.buckets["new-bucket"])
	assert.Equal(t, fs.ErrorDirNotFound, f.Rmdir(ctx, "new-bucket"))

	// Mkdir makes it again after it was removed
	require.NoError(t, f.Mkdir(ctx, "new-bucket"))
	assert.True(t, fake.buckets["new-bucket"])

	// Purge removes the bucket too
	put(ctx, t, f, "new-bucket/a.txt", "x", time.Now())
	require.NoError(t, f.Purge(ctx, "new-bucket"))
	assert.False(t, fake.buckets["new-bucket"])
	assert.Equal(t, fs.ErrorDirNotFound, f.Purge(ctx, "new-bucket"))

	// Uploads and server-side copies make the bucket
	o := put(ctx, t, f, "put-bucket/a.txt", "x", time.Now())
	assert.True(t, fake.buckets["put-bucket"])
	_, err = f.Copy(ctx, o, "copy-bucket/a.txt")
	require.NoError(t, err)
	assert.True(t, fake.buckets["copy-bucket"])
}

func TestBucketsRestrictedToken(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret, "bucket")
	f, _ := newTestFsRoot(ctx, t, fake, "", configmap.Simple{"allowed_methods": "GET,PUT"})

	// A token which can't create buckets can still use existing ones
	require.NoError(t, f.Mkdir(ctx, "bucket"))
	put(ctx, t, f, "bucket/a.txt", "x", time.Now())

	// but can't use missing ones
	err := f.Mkdir(ctx, "missing")
	assert.Equal(t, http.StatusForbidden, statusCode(err))
}

func TestBucketsScopedToken(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret, "bucket", "other")
	f, _ := newTestFsRoot(ctx, t, fake, "", configmap.Simple{"allowed_prefixes": "bucket/,gone/"})

	// Listing and making buckets needs an unscoped token
	_, err := f.List(ctx, "")
	assert.Equal(t, http.StatusForbidden, statusCode(err))

	// but a scoped token can use the buckets in its scope
	fake.Reset()
	require.NoError(t, f.Mkdir(ctx, "bucket"))
	assert.Equal(t, []string{"POST /api/v1/buckets", "GET /api/v1/buckets/bucket"}, requestPaths(fake))
	put(ctx, t, f, "bucket/a.txt", "x", time.Now())
	assert.Equal(t, fs.ErrorDirectoryNotEmpty, f.Rmdir(ctx, "bucket"))
	delete(fake.files, "bucket/a.txt")
	require.NoError(t, f.Rmdir(ctx, "bucket"))
	assert.False(t, fake.buckets["bucket"])

	// A missing bucket is found to be missing although the token
	// can't list the buckets to see the endpoints are there
	assert.Equal(t, fs.ErrorDirNotFound, f.removeBucket(ctx, "gone"))

	// and it can't use those out of its scope
	_, err = f.List(ctx, "other")
	assert.Equal(t, http.StatusForbidden, statusCode(err))
}

func TestBucketsEmptyFolders(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret, "bucket")
	f, _ := newTestFsRoot(ctx, t, fake, "", nil)

	// The server deletes a bucket holding only empty folders
	require.NoError(t, f.Mkdir(ctx, "bucket/dir"))
	require.NoError(t, f.removeBucket(ctx, "bucket"))
	assert.False(t, fake.buckets["bucket"])
	assert.Empty(t, fake.folders)

	// so Purge can remove it
	require.NoError(t, f.Mkdir(ctx, "bucket/dir"))
	require.NoError(t, f.Purge(ctx, "bucket"))
	assert.False(t, fake.buckets["bucket"])
}

func TestBucketsOldServer(t *testing.T) {
	ctx := context.Background()
	fake := newFakeServer(testSecret, "bucket").oldServer()
	f, _ := newTestFsRoot(ctx, t, fake, "", nil)

	// A 404 from deleting a bucket could be the bucket or the
	// endpoint which is missing so it must check which
	require.NoError(t, f.Rmdir(ctx, "bucket"))
	assert.True(t, fake.buckets["bucket"])

	// Buckets can't be made so they must exist already
	require.NoError(t, f.Mkdir(ctx, "bucket"))
	require.NoError(t, f.Mkdir(ctx, "missing"))
	assert.False(t, fake.buckets["missing"])

	// The root lists the buckets with the file endpoints
	entries, err := f.List(ctx, "")
	require.NoError(t, err)
	assert.Len(t, entries, 1)

	// Buckets can't be removed so they are left
	require.NoError(t, f.Rmdir(ctx, "bucket"))
	assert.True(t, fake.buckets["bucket"])
	put(ctx, t, f, "bucket/a.txt", "x", time.Now())
	assert.Equal(t, fs.ErrorDirectoryNotEmpty, f.Rmdir(ctx, "bucket"))

	// Once known the bucket endpoints aren't tried again
	fake.Reset()
	require.NoError(t, f.Mkdir(ctx, "another"))
	_, err = f.List(ctx, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"GET /list"}, requestPaths(fake))
}
