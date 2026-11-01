package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// kamplexfsTusFake holds the resumable uploads of the fake server
type kamplexfsTusFake struct {
	sessions   map[string]*kamplexfsTusSession // by upload ID
	opened     int                             // sessions opened
	openStatus int                             // status for opening sessions if not 0
	cuts       []int                           // cut the next PATCHes after this many bytes
	staleHeads int                             // HEADs after a cut showing the offset before it
	loseLast   bool                            // drop the answer to the PATCH completing an upload
	failStore  int                             // fail to store this many completed uploads
	expire     int                             // expire the session at this PATCH if not 0
	etagWrong  bool                            // completing PATCHes return a wrong ETag
	headSkew   int64                           // added to the offset HEADs show
	stuck      bool                            // PATCHes keep nothing
	patches    int                             // PATCHes received
	received   int64                           // bytes kept from PATCHes
}

// kamplexfsTusSession is a resumable upload of the fake server
type kamplexfsTusSession struct {
	name   string      // bucket/key
	meta   http.Header // headers stored on the object
	length int64       // -1 if not known
	data   []byte
	stale  int   // HEADs left to show the offset before a cut
	before int64 // offset before the cut
}

// kamplexfsResumableFeatures are the capabilities of a server with
// resumable uploads
const kamplexfsResumableFeatures = `{"version":"1.4.0","storageMode":"packed-volume","maxKeys":1000,
"features":["rename-object","rename-prefix","sequential-multipart","mtime-nanos","checksums","resumable-upload","md5"]}`

// newKamPlexFSResumableFake makes a fake server with resumable uploads
func newKamPlexFSResumableFake(t *testing.T) *kamplexfsFake {
	k := newKamPlexFSFake(t, false)
	k.capsBody = kamplexfsResumableFeatures
	return k
}

// setLength sets the length headers of an answer about s
func (s *kamplexfsTusSession) setLength(w http.ResponseWriter) {
	if s.length < 0 {
		w.Header().Set("Upload-Defer-Length", "1")
	} else {
		w.Header().Set("Upload-Length", strconv.FormatInt(s.length, 10))
	}
}

// tusOpen opens a resumable upload of name
func (k *kamplexfsFake) tusOpen(w http.ResponseWriter, r *http.Request, name string) {
	assert.NotEmpty(k.t, r.Header.Get("Authorization"), "opening a session must be signed")
	body, err := io.ReadAll(r.Body)
	require.NoError(k.t, err)
	assert.Empty(k.t, body)
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.tus.openStatus != 0 {
		writeS3Error(w, k.tus.openStatus, "NotImplemented")
		return
	}
	s := &kamplexfsTusSession{name: name, meta: http.Header{}, length: -1}
	if v := r.URL.Query().Get("kamplexfsUploadLength"); v != "" {
		s.length, err = strconv.ParseInt(v, 10, 64)
		require.NoError(k.t, err)
	}
	// Only these are stored by opening a session
	for _, name := range []string{"Content-Type", "X-Amz-Meta-Mtime", "X-Amz-Tagging"} {
		if v := r.Header.Get(name); v != "" {
			s.meta.Set(name, v)
		}
	}
	if k.tus.sessions == nil {
		k.tus.sessions = map[string]*kamplexfsTusSession{}
	}
	k.tus.opened++
	id := fmt.Sprintf("upload-%d", k.tus.opened)
	k.tus.sessions[id] = s
	location := k.srv.URL + "/.tus/" + id + "?uploadToken=token-" + id
	w.Header().Set("Location", location)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	require.NoError(k.t, json.NewEncoder(w).Encode(map[string]string{"uploadId": id, "location": location}))
}

// hangUp closes the connection without an answer
func hangUp(t *testing.T, w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

// tusServe answers the TUS requests to the sessions
func (k *kamplexfsFake) tusServe(w http.ResponseWriter, r *http.Request) {
	assert.Empty(k.t, r.Header.Get("Authorization"), "TUS requests must not be signed")
	if r.Header.Get("Tus-Resumable") != kamplexfsTusVersion {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/.tus/")
	k.mu.Lock()
	s := k.tus.sessions[id]
	k.mu.Unlock()
	if s == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.URL.Query().Get("uploadToken") != "token-"+id || len(r.URL.Query()) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch r.Method {
	case http.MethodHead:
		k.mu.Lock()
		offset := int64(len(s.data))
		if s.stale > 0 {
			s.stale--
			offset = s.before
		}
		k.mu.Unlock()
		w.Header().Set("Upload-Offset", strconv.FormatInt(offset+k.tus.headSkew, 10))
		s.setLength(w)
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		k.mu.Lock()
		delete(k.tus.sessions, id)
		k.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		k.tusPatch(w, r, id, s)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// tusPatch appends the body of a PATCH to session s
func (k *kamplexfsFake) tusPatch(w http.ResponseWriter, r *http.Request, id string, s *kamplexfsTusSession) {
	if r.Header.Get("Content-Type") != "application/offset+octet-stream" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	k.mu.Lock()
	k.tus.patches++
	patch := k.tus.patches
	expire := k.tus.expire == patch
	cut := -1
	if len(k.tus.cuts) > 0 {
		cut, k.tus.cuts = k.tus.cuts[0], k.tus.cuts[1:]
	}
	offset := int64(len(s.data))
	k.mu.Unlock()
	if expire {
		k.mu.Lock()
		delete(k.tus.sessions, id)
		k.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.Header.Get("Upload-Offset") != strconv.FormatInt(offset, 10) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusConflict)
		return
	}
	if v := r.Header.Get("Upload-Length"); v != "" {
		length, err := strconv.ParseInt(v, 10, 64)
		if err != nil || (s.length >= 0 && length != s.length) || length < offset {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.length = length
	}
	if k.tus.stuck {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Upload-Offset", strconv.FormatInt(offset, 10))
		s.setLength(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if cut >= 0 {
		// Keep what arrived before the connection broke off
		data := make([]byte, cut)
		n, _ := io.ReadFull(r.Body, data)
		k.mu.Lock()
		s.data = append(s.data, data[:n]...)
		k.tus.received += int64(n)
		s.stale, s.before = k.tus.staleHeads, offset
		k.mu.Unlock()
		hangUp(k.t, w)
		return
	}
	// Keep what arrived if the client broke off the body
	data, err := io.ReadAll(r.Body)
	k.mu.Lock()
	s.data = append(s.data, data...)
	k.tus.received += int64(len(data))
	if err != nil {
		k.mu.Unlock()
		return
	}
	offset = int64(len(s.data))
	complete := s.length >= 0 && offset == s.length
	failStore := complete && k.tus.failStore > 0
	if failStore {
		k.tus.failStore--
	}
	if complete && !failStore {
		delete(k.tus.sessions, id)
	}
	k.mu.Unlock()
	if s.length >= 0 && offset > s.length {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}
	if failStore {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if complete {
		o := &kamplexfsObject{meta: s.meta}
		k.setData(o, s.data)
		k.storeObject(o, s.name)
		if k.tus.loseLast {
			hangUp(k.t, w)
			return
		}
		etag := o.etag
		if k.tus.etagWrong {
			etag = strings.Repeat("0", 32)
		}
		w.Header().Set("ETag", `"`+etag+`"`)
		k.blake3Header(w, o)
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(offset, 10))
	s.setLength(w)
	w.WriteHeader(http.StatusNoContent)
}

// tusRequests returns the requests to the sessions with method
func (k *kamplexfsFake) tusRequests(method string) (out []kamplexfsRequest) {
	k.mu.Lock()
	defer k.mu.Unlock()
	for _, r := range k.requests {
		if strings.HasPrefix(r.Path, "/.tus/") && r.Method == method {
			out = append(out, r)
		}
	}
	return out
}

// resumableData returns n bytes of test data
func resumableData(n int) []byte {
	data := make([]byte, n)
	_, _ = rand.New(rand.NewSource(int64(n))).Read(data)
	return data
}

// md5Hex returns the MD5 of data
func md5Hex(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

// putResumable uploads data as file.bin, of unknown size if unknown
// is set.
func putResumable(ctx context.Context, f fs.Fs, data []byte, unknown bool, options ...fs.OpenOption) (fs.Object, error) {
	size := int64(len(data))
	if unknown {
		size = -1
	}
	src := object.NewStaticObjectInfo("file.bin", time.Unix(1709608272, 123456789), size, true, nil, nil)
	return f.Put(ctx, bytes.NewReader(data), src, options...)
}

// checkStored checks the fake stored data as file.bin
func checkStored(t *testing.T, k *kamplexfsFake, data []byte) {
	k.mu.Lock()
	o := k.objects["bucket/file.bin"]
	k.mu.Unlock()
	require.NotNil(t, o, "object not stored")
	assert.Equal(t, int64(len(data)), o.size)
	if k.noMD5 {
		assert.Equal(t, "b3-"+blake3Hex(data), o.etag)
		return
	}
	// Multipart ETags of the fake are the MD5 with the part count
	etag, _, _ := strings.Cut(o.etag, "-")
	assert.Equal(t, md5Hex(data), etag)
}

// resumableCutoff makes uploads above 5 MiB resumable in 5 MiB chunks
var resumableCutoff = configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M"}

func TestKamPlexFSResumableUpload(t *testing.T) {
	ctx := context.Background()
	const chunk = 5 * 1024 * 1024
	for _, test := range []struct {
		name    string
		size    int
		unknown bool
		extra   configmap.Simple
		patches int
	}{
		{name: "Known", size: 2*chunk + 1234, patches: 3},
		{name: "Unknown", size: 2*chunk + 1234, unknown: true, patches: 4},
		{name: "KnownChunks", size: 2 * chunk, patches: 2},
		{name: "UnknownChunks", size: 2 * chunk, unknown: true, patches: 3},
		{name: "UnknownSmall", size: 1234, unknown: true, patches: 2},
		{name: "EmptyUnknown", unknown: true, patches: 1},
		{name: "EmptyKnown", extra: configmap.Simple{"upload_cutoff": "0"}, patches: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			extra := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M"}
			for key, value := range test.extra {
				extra[key] = value
			}
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, extra)
			data := resumableData(test.size)
			o, err := putResumable(ctx, f, data, test.unknown)
			require.NoError(t, err)
			checkStored(t, k, data)
			assert.Equal(t, int64(len(data)), o.Size())
			sum, err := o.Hash(ctx, hash.MD5)
			require.NoError(t, err)
			assert.Equal(t, md5Hex(data), sum)

			opens := k.find("kamplexfsResumable")
			require.Len(t, opens, 1)
			query, err := url.ParseQuery(opens[0].Query)
			require.NoError(t, err)
			assert.True(t, query.Has("kamplexfsResumable"))
			if test.unknown {
				assert.False(t, query.Has("kamplexfsUploadLength"))
			} else {
				assert.Equal(t, strconv.Itoa(len(data)), query.Get("kamplexfsUploadLength"))
			}
			assert.Equal(t, "/s3/bucket/file.bin", opens[0].Path)
			assert.Equal(t, "application/octet-stream", opens[0].Header.Get("Content-Type"))
			assert.Empty(t, k.find("uploads"), "must not be a multipart upload")
			assert.Empty(t, k.find("x-id=PutObject"), "must not be a single part upload")
			assert.Equal(t, 0, k.count(http.MethodHead), "must not HEAD")

			patches := k.tusRequests(http.MethodPatch)
			require.Len(t, patches, test.patches)
			var offset int64
			for i, r := range patches {
				assert.Equal(t, kamplexfsTusVersion, r.Header.Get("Tus-Resumable"))
				assert.Equal(t, strconv.FormatInt(offset, 10), r.Header.Get("Upload-Offset"))
				length := r.Header.Get("Upload-Length")
				if test.unknown && i == len(patches)-1 {
					assert.Equal(t, strconv.Itoa(len(data)), length, "last PATCH must set the length")
				} else {
					assert.Empty(t, length)
				}
				n := min(int64(chunk), int64(len(data))-offset)
				if test.unknown && i == len(patches)-1 {
					n = 0
				}
				if n > 0 && !test.unknown {
					assert.Equal(t, strconv.FormatInt(n, 10), r.Header.Get("Content-Length"))
				}
				offset += n
			}
			k.mu.Lock()
			assert.Empty(t, k.tus.sessions)
			assert.Equal(t, int64(len(data)), k.tus.received)
			k.mu.Unlock()
		})
	}
}

// TestKamPlexFSResumableCopy checks a copy of a file above the
// multi-thread cutoff, as rclone mount writes back, is resumable.
func TestKamPlexFSResumableCopy(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MultiThreadCutoff = 1024 * 1024
	ci.MultiThreadStreams = 4
	k := newKamPlexFSResumableFake(t)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	dir := t.TempDir()
	data := resumableData(6 * 1024 * 1024)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "file.bin"), data, 0o666))
	local, err := fs.NewFs(ctx, dir)
	require.NoError(t, err)
	src, err := local.NewObject(ctx, "file.bin")
	require.NoError(t, err)
	_, err = operations.Copy(ctx, f, nil, "file.bin", src)
	require.NoError(t, err)
	checkStored(t, k, data)
	assert.Len(t, k.find("kamplexfsResumable"), 1)
	assert.Empty(t, k.find("uploads"), "must not be a multipart upload")
}

func TestKamPlexFSResumableModTime(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	extra := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M", "kamplexfs_exact_modtime": "true"}
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, extra)
	data := resumableData(6 * 1024 * 1024)
	modTime := time.Unix(1709608272, 123456789)
	o, err := putResumable(ctx, f, data, false)
	require.NoError(t, err)
	assert.Equal(t, modTime, o.ModTime(ctx))
	opens := k.find("kamplexfsResumable")
	require.Len(t, opens, 1)
	assert.Equal(t, swift.TimeToFloatString(modTime), opens[0].Header.Get("X-Amz-Meta-Mtime"))

	o, err = f.NewObject(ctx, "file.bin")
	require.NoError(t, err)
	assert.Equal(t, modTime, o.ModTime(ctx))
}

func TestKamPlexFSResumableResume(t *testing.T) {
	ctx := context.Background()
	const chunk = 5 * 1024 * 1024
	for _, test := range []struct {
		name       string
		cuts       []int
		staleHeads int
		heads      int // HEADs of the session
	}{
		{name: "CutFirst", cuts: []int{1024 * 1024}, heads: 1},
		{name: "CutTwice", cuts: []int{1024 * 1024, 1024}, heads: 2},
		{name: "CutAtStart", cuts: []int{0}, heads: 1},
		{name: "CutSecondChunk", cuts: []int{-1, 3 * 1024 * 1024}, heads: 1},
		{name: "StaleHead", cuts: []int{1024 * 1024}, staleHeads: 1, heads: 2},
		{name: "StaleHeadTwice", cuts: []int{1024 * 1024}, staleHeads: 2, heads: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			k.tus.cuts = test.cuts
			k.tus.staleHeads = test.staleHeads
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
			data := resumableData(2*chunk + 1234)
			_, err := putResumable(ctx, f, data, false)
			require.NoError(t, err)
			checkStored(t, k, data)
			assert.Len(t, k.tusRequests(http.MethodHead), test.heads)
			assert.Len(t, k.find("kamplexfsResumable"), 1, "must resume the same session")
			k.mu.Lock()
			assert.Equal(t, int64(len(data)), k.tus.received, "must only resend what didn't arrive")
			k.mu.Unlock()
		})
	}
}

// TestKamPlexFSResumableRetries checks the retries count again each
// time the server has more.
func TestKamPlexFSResumableRetries(t *testing.T) {
	ctx := context.Background()
	ctx, ci := fs.AddConfig(ctx)
	ci.LowLevelRetries = 2
	const mib = 1024 * 1024
	for _, test := range []struct {
		name    string
		cuts    []int
		wantErr bool
	}{
		{name: "Progress", cuts: []int{mib, mib, mib, mib}},
		{name: "NoProgress", cuts: []int{mib, 0, 0}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			k.tus.cuts = test.cuts
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
			data := resumableData(6 * mib)
			_, err := putResumable(ctx, f, data, false)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			checkStored(t, k, data)
		})
	}
}

// TestKamPlexFSResumableStuck checks a server which keeps nothing
// fails the upload.
func TestKamPlexFSResumableStuck(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.stuck = true
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	_, err := putResumable(ctx, f, resumableData(6*1024*1024), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server is stuck at offset 0")
}

// TestKamPlexFSResumableBadOffset checks an offset outside the data
// held fails the upload.
func TestKamPlexFSResumableBadOffset(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.cuts = []int{1024}
	k.tus.headSkew = 5 * 1024 * 1024
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	_, err := putResumable(ctx, f, resumableData(11*1024*1024), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server has 5243904 bytes but 0 to 5242880 are held")
	assert.Len(t, k.tusRequests(http.MethodHead), 1, "must not retry")
	assert.Len(t, k.tusRequests(http.MethodDelete), 1, "must delete the session")
}

func TestKamPlexFSResumableUnknownResume(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.cuts = []int{-1, 1024, -1, 0}
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	data := resumableData(2*5*1024*1024 + 1234)
	o, err := putResumable(ctx, f, data, true)
	require.NoError(t, err)
	checkStored(t, k, data)
	assert.Equal(t, int64(len(data)), o.Size())
	assert.Len(t, k.tusRequests(http.MethodHead), 2)
}

func TestKamPlexFSResumableLostAnswer(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name    string
		unknown bool
		blake3  bool // the server has BLAKE3 but not MD5
		pending bool // the server hasn't computed the BLAKE3 yet
		extra   configmap.Simple
	}{
		{name: "Known"},
		{name: "Unknown", unknown: true},
		{name: "BLAKE3", blake3: true},
		{name: "BLAKE3Pending", blake3: true, pending: true},
		{name: "BLAKE3DisableChecksum", blake3: true,
			extra: configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M", "disable_checksum": "true"}},
	} {
		unknown := test.unknown
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			if test.blake3 {
				k.capsBody = strings.Replace(kamplexfsResumableFeatures, `,"md5"`, `,"blake3"`, 1)
				k.noMD5, k.blake3, k.blake3Pending = true, true, test.pending
			}
			k.tus.loseLast = true
			extra := resumableCutoff
			if test.extra != nil {
				extra = test.extra
			}
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, extra)
			data := resumableData(6 * 1024 * 1024)
			o, err := putResumable(ctx, f, data, unknown)
			require.NoError(t, err)
			checkStored(t, k, data)
			assert.Equal(t, int64(len(data)), o.Size())
			// HEADs of the session, which has gone, and the object,
			// which is HEADed again for a BLAKE3 not computed yet
			heads := 2
			if test.pending {
				heads = 3
			}
			assert.Len(t, k.tusRequests(http.MethodHead), 1)
			assert.Equal(t, heads, k.count(http.MethodHead))
		})
	}
}

func TestKamPlexFSResumableExpired(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.expire = 2
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	_, err := putResumable(ctx, f, resumableData(11*1024*1024), false)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errKamplexfsSessionGone), err)
	assert.Empty(t, k.tusRequests(http.MethodDelete), "the session has gone already")
	k.mu.Lock()
	assert.Empty(t, k.objects)
	k.mu.Unlock()
}

func TestKamPlexFSResumableLostAnswerNotStored(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name   string
		old    []byte // object stored before
		noMD5  bool
		blake3 bool // the server has BLAKE3 but not MD5
	}{
		{name: "NoObject"},
		{name: "OtherSize", old: []byte("old")},
		{name: "OtherSizeNoMD5", old: []byte("old"), noMD5: true},
		{name: "OtherData", old: resumableData(6*1024*1024 + 1)[1:]},
		{name: "OtherDataBLAKE3", old: resumableData(6*1024*1024 + 1)[1:], blake3: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			switch {
			case test.noMD5:
				k.capsBody = strings.Replace(kamplexfsResumableFeatures, `,"md5"`, ``, 1)
			case test.blake3:
				k.capsBody = strings.Replace(kamplexfsResumableFeatures, `,"md5"`, `,"blake3"`, 1)
				k.noMD5, k.blake3 = true, true
			}
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
			if test.old != nil {
				_, err := putResumable(ctx, f, test.old, false)
				require.NoError(t, err)
				k.reset()
			}
			// The PATCH completing the upload expires the session instead
			k.tus.expire = k.tus.patches + 2
			_, err := putResumable(ctx, f, resumableData(6*1024*1024), false)
			require.Error(t, err)
			assert.True(t, fserrors.IsRetryError(err), "must retry the whole upload: %v", err)
			assert.Equal(t, 1, k.count(http.MethodHead), "must HEAD the object")
		})
	}
}

func TestKamPlexFSResumableStoreFailed(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.failStore = 1
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	data := resumableData(6 * 1024 * 1024)
	_, err := putResumable(ctx, f, data, false)
	require.NoError(t, err)
	checkStored(t, k, data)
	patches := k.tusRequests(http.MethodPatch)
	require.Len(t, patches, 3)
	last := patches[2]
	assert.Equal(t, strconv.Itoa(len(data)), last.Header.Get("Upload-Offset"))
	assert.Equal(t, "0", last.Header.Get("Content-Length"))
	assert.Len(t, k.tusRequests(http.MethodHead), 1)
}

func TestKamPlexFSResumableChecks(t *testing.T) {
	ctx := context.Background()
	data := resumableData(6 * 1024 * 1024)

	t.Run("MD5Mismatch", func(t *testing.T) {
		k := newKamPlexFSResumableFake(t)
		k.tus.etagWrong = true
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
		_, err := putResumable(ctx, f, data, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MD5 differ")
	})

	t.Run("MD5MismatchDisableChecksum", func(t *testing.T) {
		k := newKamPlexFSResumableFake(t)
		k.tus.etagWrong = true
		extra := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M", "disable_checksum": "true"}
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, extra)
		_, err := putResumable(ctx, f, data, false)
		require.NoError(t, err)
	})

	t.Run("NoMD5", func(t *testing.T) {
		k := newKamPlexFSResumableFake(t)
		k.capsBody = strings.Replace(kamplexfsResumableFeatures, `,"md5"`, `,"blake3"`, 1)
		k.noMD5, k.blake3 = true, true
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
		_, err := putResumable(ctx, f, data, false)
		require.NoError(t, err, "must not compare the ETag with an MD5")
	})

	for _, test := range []struct {
		name    string
		pending bool
		wrong   bool
		heads   int
	}{
		{name: "BLAKE3"},
		{name: "BLAKE3Mismatch", wrong: true},
		{name: "BLAKE3Pending", pending: true, heads: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			k.capsBody = strings.Replace(kamplexfsResumableFeatures, `"md5"`, `"md5","blake3"`, 1)
			k.blake3, k.blake3Pending, k.blake3Wrong = true, test.pending, test.wrong
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
			o, err := putResumable(ctx, f, data, false)
			if test.wrong {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "BLAKE3 differ")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.heads, k.count(http.MethodHead))
			k.reset()
			digest, err := o.Hash(ctx, hash.BLAKE3)
			require.NoError(t, err)
			if test.pending {
				assert.Empty(t, digest)
			} else {
				assert.Equal(t, blake3Hex(data), digest)
			}
			assert.Empty(t, k.requests, "the upload must have set the BLAKE3")
		})
	}
}

// errorReader returns data and then an error
type errorReader struct {
	data   []byte
	cancel func() // if set, cancels instead of returning an error
}

func (r *errorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		if r.cancel != nil {
			r.cancel()
			return 0, context.Canceled
		}
		return 0, errors.New("source broke")
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestKamPlexFSResumableUpdateBlake3 checks a resumable upload
// doesn't keep the BLAKE3 of the data it replaced.
func TestKamPlexFSResumableUpdateBlake3(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.capsBody = strings.Replace(kamplexfsResumableFeatures, `"md5"`, `"md5","blake3"`, 1)
	k.blake3 = true
	extra := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M", "no_head": "true"}
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, extra)
	o, err := putResumable(ctx, f, []byte("hello"), false)
	require.NoError(t, err)
	digest, err := o.Hash(ctx, hash.BLAKE3)
	require.NoError(t, err)
	require.Equal(t, blake3Hex([]byte("hello")), digest)

	k.blake3Pending = true
	data := resumableData(6 * 1024 * 1024)
	src := object.NewStaticObjectInfo("file.bin", time.Now(), int64(len(data)), true, nil, nil)
	require.NoError(t, o.Update(ctx, bytes.NewReader(data), src))
	require.Len(t, k.find("kamplexfsResumable"), 1)
	digest, err = o.Hash(ctx, hash.BLAKE3)
	require.NoError(t, err)
	assert.Empty(t, digest)
}

func TestKamPlexFSResumableAbort(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("Cancel=%v", cancel), func(t *testing.T) {
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()
			k := newKamPlexFSResumableFake(t)
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
			in := &errorReader{data: resumableData(7 * 1024 * 1024)}
			if cancel {
				in.cancel = cancelCtx
			}
			src := object.NewStaticObjectInfo("file.bin", time.Now(), 11*1024*1024, true, nil, nil)
			_, err := f.Put(ctx, in, src)
			require.Error(t, err)
			if !cancel {
				assert.Contains(t, err.Error(), "source broke")
			}
			assert.Len(t, k.tusRequests(http.MethodDelete), 1, "must delete the session")
			k.mu.Lock()
			assert.Empty(t, k.tus.sessions)
			assert.Empty(t, k.objects)
			k.mu.Unlock()
		})
	}
}

func TestKamPlexFSResumableShortSource(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	data := resumableData(7 * 1024 * 1024)
	src := object.NewStaticObjectInfo("file.bin", time.Now(), 11*1024*1024, true, nil, nil)
	_, err := f.Put(ctx, bytes.NewReader(data), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "source is 7340032 bytes, expecting 11534336")
	assert.Len(t, k.tusRequests(http.MethodDelete), 1, "must delete the session")
}

func TestKamPlexFSResumableTokenNotInErrors(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.cuts = []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	ctx, ci := fs.AddConfig(ctx)
	ci.LowLevelRetries = 2
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	_, err := putResumable(ctx, f, resumableData(6*1024*1024), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/.tus/upload-1")
	assert.NotContains(t, err.Error(), "token-")
}

func TestKamPlexFSResumableWhen(t *testing.T) {
	ctx := context.Background()
	const chunk = 5 * 1024 * 1024
	big := resumableData(chunk + 1234)
	small := resumableData(1234)
	withConcurrency := func(mode string, concurrency string, extra ...string) configmap.Simple {
		m := configmap.Simple{"upload_cutoff": "5M", "chunk_size": "5M", "upload_concurrency": concurrency}
		if mode != "" {
			m["kamplexfs_resumable"] = mode
		}
		for i := 0; i < len(extra); i += 2 {
			m[extra[i]] = extra[i+1]
		}
		return m
	}
	for _, test := range []struct {
		name      string
		caps      string
		extra     configmap.Simple
		data      []byte
		unknown   bool
		metadata  bool // --metadata
		options   []fs.OpenOption
		tagging   string // tags stored
		resumable bool
	}{
		{name: "AutoConcurrency1", extra: withConcurrency("", "1"), data: big, resumable: true},
		{name: "AutoConcurrency4", extra: withConcurrency("auto", "4"), data: big},
		{name: "AutoUnknown", extra: withConcurrency("auto", "4"), data: big, unknown: true, resumable: true},
		{name: "AutoSmall", extra: withConcurrency("auto", "1"), data: small},
		{name: "AlwaysConcurrency4", extra: withConcurrency("always", "4"), data: big, resumable: true},
		{name: "AlwaysSmall", extra: withConcurrency("always", "4"), data: small},
		{name: "Never", extra: withConcurrency("never", "1"), data: big},
		{name: "NeverUnknown", extra: withConcurrency("never", "1"), data: big, unknown: true},
		{name: "OldServer", caps: kamplexfsAllFeatures, extra: withConcurrency("always", "1"), data: big},
		{name: "OldServerUnknown", caps: kamplexfsAllFeatures, extra: withConcurrency("always", "1"), data: big, unknown: true},
		{name: "CacheControl", extra: withConcurrency("always", "1"), data: big, resumable: true,
			options: []fs.OpenOption{&fs.HTTPOption{Key: "Cache-Control", Value: "no-cache"}}},
		{name: "UserMetadata", extra: withConcurrency("always", "1"), data: big, metadata: true, resumable: true,
			options: []fs.OpenOption{fs.MetadataOption{"color": "blue"}}},
		{name: "ACL", extra: withConcurrency("always", "1", "acl", "private"), data: big, resumable: true},
		{name: "StorageClass", extra: withConcurrency("always", "1", "storage_class", "STANDARD"), data: big, resumable: true},
		{name: "Conditional", extra: withConcurrency("always", "1"), data: big,
			options: []fs.OpenOption{&fs.HTTPOption{Key: "If-None-Match", Value: "*"}}},
		{name: "Encrypted", extra: withConcurrency("always", "1", "server_side_encryption", "AES256"), data: big},
		{name: "Tagging", extra: withConcurrency("always", "1"), data: big, resumable: true, tagging: "a=b",
			options: []fs.OpenOption{&fs.HTTPOption{Key: "X-Amz-Tagging", Value: "a=b"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			if test.caps != "" {
				k.capsBody = test.caps
			}
			ctx := ctx
			if test.metadata {
				var ci *fs.ConfigInfo
				ctx, ci = fs.AddConfig(ctx)
				ci.Metadata = true
			}
			f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, test.extra)
			_, err := putResumable(ctx, f, test.data, test.unknown, test.options...)
			require.NoError(t, err)
			checkStored(t, k, test.data)
			opens := k.find("kamplexfsResumable")
			if !test.resumable {
				assert.Empty(t, opens, "must not use a resumable upload")
				return
			}
			require.Len(t, opens, 1)
			k.mu.Lock()
			assert.Equal(t, test.tagging, k.objects["bucket/file.bin"].meta.Get("X-Amz-Tagging"))
			k.mu.Unlock()
			assert.Empty(t, k.find("uploads"), "must not be a multipart upload")
		})
	}
}

func TestKamPlexFSResumableNotImplemented(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	k.tus.openStatus = http.StatusNotImplemented
	f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, resumableCutoff)
	data := resumableData(6 * 1024 * 1024)
	for range 2 {
		_, err := putResumable(ctx, f, data, false)
		require.NoError(t, err)
		checkStored(t, k, data)
	}
	assert.Len(t, k.find("kamplexfsResumable"), 1, "must stop trying after a 501")
	assert.Len(t, k.find("uploads"), 2, "must upload with multipart")
}

func TestKamPlexFSResumableOption(t *testing.T) {
	ctx := context.Background()
	k := newKamPlexFSResumableFake(t)
	regInfo, err := fs.Find("s3")
	require.NoError(t, err)

	t.Run("Config", func(t *testing.T) {
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, configmap.Simple{"kamplexfs_resumable": "always"})
		assert.Equal(t, kamplexfsResumableAlways, f.kpx.opt.Resumable)
	})
	t.Run("Default", func(t *testing.T) {
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
		assert.Equal(t, kamplexfsResumableAuto, f.kpx.opt.Resumable)
	})
	t.Run("EnvVar", func(t *testing.T) {
		t.Setenv("RCLONE_S3_KAMPLEXFS_RESUMABLE", "never")
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
		assert.Equal(t, kamplexfsResumableNever, f.kpx.opt.Resumable)
	})
	t.Run("Flag", func(t *testing.T) {
		opt := regInfo.Options.Get("kamplexfs_resumable")
		require.NotNil(t, opt)
		// This is what --s3-kamplexfs-resumable does
		require.NoError(t, opt.Set("never"))
		defer func() { opt.Value = nil }()
		f := newKamPlexFSTestFs(ctx, t, k.endpoint(), kamplexfsProvider, nil)
		assert.Equal(t, kamplexfsResumableNever, f.kpx.opt.Resumable)
	})
	t.Run("ConnectionString", func(t *testing.T) {
		remote := fmt.Sprintf(":s3,provider=KamPlexFS,endpoint='%s',access_key_id=AKID,secret_access_key=SECRET,no_check_bucket,kamplexfs_resumable=never:bucket", k.endpoint())
		f, err := fs.NewFs(ctx, remote)
		require.NoError(t, err)
		assert.Equal(t, kamplexfsResumableNever, f.(*Fs).kpx.opt.Resumable)
	})
	t.Run("Invalid", func(t *testing.T) {
		m := fs.ConfigMap("s3", regInfo.Options, "TestKamPlexFS", configmap.Simple{
			"provider":            kamplexfsProvider,
			"endpoint":            k.endpoint(),
			"kamplexfs_resumable": "sometimes",
		})
		_, err := NewFs(ctx, "TestKamPlexFS", "bucket", m)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sometimes")
	})
	t.Run("Help", func(t *testing.T) {
		opt := regInfo.Options.Get("kamplexfs_resumable")
		require.NotNil(t, opt)
		assert.True(t, opt.Advanced)
		assert.Equal(t, kamplexfsProvider, opt.Provider)
		assert.Equal(t, "auto", opt.Default.(fmt.Stringer).String())
	})
}

func TestKamPlexFSResumableChunkWriter(t *testing.T) {
	ctx := context.Background()
	multiThread, ci := fs.AddConfig(ctx)
	ci.MultiThreadSet = true
	for _, test := range []struct {
		name  string
		ctx   context.Context
		caps  string
		extra configmap.Simple
		want  bool
	}{
		{name: "Resumable", ctx: ctx},
		{name: "MultiThreadSet", ctx: multiThread, want: true},
		{name: "Concurrency4", ctx: ctx, extra: configmap.Simple{"upload_concurrency": "4"}, want: true},
		{name: "AlwaysConcurrency4", ctx: ctx, extra: configmap.Simple{"upload_concurrency": "4", "kamplexfs_resumable": "always"}},
		{name: "Never", ctx: ctx, extra: configmap.Simple{"kamplexfs_resumable": "never"}, want: true},
		{name: "OldServer", ctx: ctx, caps: kamplexfsAllFeatures, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			k := newKamPlexFSResumableFake(t)
			if test.caps != "" {
				k.capsBody = test.caps
			}
			f := newKamPlexFSTestFs(test.ctx, t, k.endpoint(), kamplexfsProvider, test.extra)
			assert.Equal(t, test.want, f.Features().OpenChunkWriter != nil)
		})
	}
}
