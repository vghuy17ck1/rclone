package kamplexfs

// An in-memory fake of the KamPlexFS JSON REST API used to test the
// backend without a real server.

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/backend/kamplexfs/api"
)

// fakeFile is a stored object
type fakeFile struct {
	id      int
	data    []byte
	mtime   time.Time
	created time.Time
	pending bool // report the MD5 as PENDING
}

// fakeServer is the fake KamPlexFS server
type fakeServer struct {
	route  string
	secret []byte
	maxTTL time.Duration

	// server features and modes
	packed    bool // packed-volume mode: no empty folders
	nanos     bool // mtimes have sub-second precision
	patch     bool // PATCH sets the mtime
	recursive bool // recursive listings are supported
	// recursive listings are marked and have every folder in the
	// first page
	recursiveFolders bool
	missing404       bool // move/copy of a missing source is a 404
	bucketAPI        bool // the bucket endpoints are supported
	maxPage          int  // largest page size

	mu       sync.Mutex
	nextID   int
	buckets  map[string]bool
	files    map[string]*fakeFile // keyed by bucket/key
	folders  map[string]bool      // keyed by bucket/dir/ - direct-fs only
	requests []fakeRequest
	now      func() time.Time
	// after failSkip requests, fail the next failN requests with
	// this status and body
	failSkip   int
	failN      int
	failStatus int
	failBody   string
}

// fakeRequest is a request the fake has received
type fakeRequest struct {
	Method string
	Path   string // path after the route
	Query  string
	Auth   string // Authorization header
	Status int    // status returned
}

// newFakeServer makes a fake server with the given buckets
func newFakeServer(secret string, buckets ...string) *fakeServer {
	f := &fakeServer{
		route:            defaultRoute,
		secret:           []byte(secret),
		maxTTL:           24 * time.Hour,
		nanos:            true,
		patch:            true,
		recursive:        true,
		missing404:       true,
		recursiveFolders: true,
		bucketAPI:        true,
		maxPage:          1000,
		buckets:          map[string]bool{},
		files:            map[string]*fakeFile{},
		folders:          map[string]bool{},
		now:              time.Now,
	}
	for _, b := range buckets {
		f.buckets[b] = true
	}
	return f
}

// oldServer makes f behave like a server without the new features
func (f *fakeServer) oldServer() *fakeServer {
	f.nanos = false
	f.patch = false
	f.recursive = false
	f.recursiveFolders = false
	f.missing404 = false
	f.bucketAPI = false
	return f
}

// Requests returns the requests received so far
func (f *fakeServer) Requests() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.requests)
}

// Reset forgets the requests received
func (f *fakeServer) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = nil
}

// FailNext makes the next n requests fail
func (f *fakeServer) FailNext(n, status int, body string) {
	f.FailAfter(0, n, status, body)
}

// FailAfter makes n requests fail after skip requests succeed
func (f *fakeServer) FailAfter(skip, n, status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failSkip, f.failN, f.failStatus, f.failBody = skip, n, status, body
}

// statusWriter records the status written
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := strings.CutPrefix(r.URL.Path, f.route)
	bucketsRoute := path.Join(path.Dir(f.route), "buckets")
	isBuckets := false
	if !ok && f.bucketAPI && (r.URL.Path == bucketsRoute || strings.HasPrefix(r.URL.Path, bucketsRoute+"/")) {
		// bucket requests are recorded with their full path
		p, ok, isBuckets = r.URL.Path, true, true
	}
	f.requests = append(f.requests, fakeRequest{
		Method: r.Method,
		Path:   p,
		Query:  r.URL.RawQuery,
		Auth:   r.Header.Get("Authorization"),
	})
	defer func() { f.requests[len(f.requests)-1].Status = sw.status }()
	if !ok {
		jsonError(sw, http.StatusNotFound, "NOT_FOUND", "no such route")
		return
	}
	if f.failSkip > 0 {
		f.failSkip--
	} else if f.failN > 0 {
		f.failN--
		sw.WriteHeader(f.failStatus)
		_, _ = io.WriteString(sw, f.failBody)
		return
	}
	resource := p
	for _, op := range []string{"/list/", "/content/"} {
		resource = strings.TrimPrefix(resource, op)
	}
	if listed, ok := strings.CutPrefix(p, "/list/"); ok && !strings.HasSuffix(listed, "/") {
		// a folder is scoped like the path "folder/"
		resource = listed + "/"
	}
	bucketName := strings.TrimPrefix(strings.TrimPrefix(p, bucketsRoute), "/")
	if isBuckets {
		// allowed_prefixes scope a bucket like the path "name/" and
		// the collection is only allowed to unscoped tokens
		resource = ""
		if bucketName != "" {
			resource = bucketName + "/"
		}
	}
	if !f.authorize(sw, r, resource) {
		return
	}
	if isBuckets {
		f.serveBuckets(sw, r, bucketName)
		return
	}
	f.serve(sw, r, p)
}

// plainError writes an authentication failure which isn't JSON
func plainError(w http.ResponseWriter, status int, text string) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, text)
}

// jsonError writes the JSON error envelope
func jsonError(w http.ResponseWriter, code int, status, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "status": status, "message": message},
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// authorize checks the JWT for resource, the path its allowed_prefixes
// apply to, writing an error and returning false if it isn't
// acceptable.
func (f *fakeServer) authorize(w http.ResponseWriter, r *http.Request, resource string) bool {
	tokenString, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		plainError(w, http.StatusUnauthorized, "Invalid token")
		return false
	}
	claims := jwt.MapClaims{}
	_, err := jwt.ParseWithClaims(tokenString, claims, func(*jwt.Token) (any, error) {
		return f.secret, nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithTimeFunc(f.now))
	switch {
	case errors.Is(err, jwt.ErrTokenRequiredClaimMissing):
		plainError(w, http.StatusUnauthorized, "Token exp required")
		return false
	case errors.Is(err, jwt.ErrTokenExpired):
		plainError(w, http.StatusUnauthorized, "Token expired")
		return false
	case err != nil:
		plainError(w, http.StatusUnauthorized, "Invalid token")
		return false
	}
	exp, _ := claims.GetExpirationTime()
	if exp.Sub(f.now()) > f.maxTTL {
		plainError(w, http.StatusUnauthorized, "Token exp exceeds maximum")
		return false
	}
	if methods, ok := claims["allowed_methods"].([]any); ok && !slices.Contains(methods, any(r.Method)) {
		plainError(w, http.StatusForbidden, "Token does not permit this resource")
		return false
	}
	if prefixes, ok := claims["allowed_prefixes"].([]any); ok {
		permitted := false
		for _, prefix := range prefixes {
			if s, ok := prefix.(string); ok && strings.HasPrefix(resource, s) {
				permitted = true
			}
		}
		if !permitted {
			plainError(w, http.StatusForbidden, "Token does not permit this resource")
			return false
		}
	}
	return true
}

// validPath checks for the paths the server rejects
func validPath(p string) bool {
	if strings.Contains(p, "//") {
		return false
	}
	return !slices.Contains(strings.Split(p, "/"), "..")
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request, p string) {
	switch {
	case p == "/list" && r.Method == http.MethodGet:
		var buckets []string
		for b := range f.buckets {
			buckets = append(buckets, b)
		}
		sort.Strings(buckets)
		writeJSON(w, http.StatusOK, api.BucketList{Kind: api.KindBucketList, Buckets: buckets})
		return
	case p == "/folders" && r.Method == http.MethodPost:
		f.createFolder(w, r)
		return
	case (p == "/move" || p == "/copy") && r.Method == http.MethodPost:
		f.moveCopy(w, r, p == "/move")
		return
	case p == "" && r.Method == http.MethodDelete:
		f.delete(w, r)
		return
	}
	for _, op := range []string{"/list/", "/content/"} {
		resource, ok := strings.CutPrefix(p, op)
		if !ok {
			continue
		}
		if !validPath(resource) {
			jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid path")
			return
		}
		bucket, _, _ := strings.Cut(resource, "/")
		if !f.buckets[bucket] {
			jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such bucket")
			return
		}
		switch {
		case op == "/list/" && r.Method == http.MethodGet:
			f.list(w, r, resource)
		case op == "/content/" && r.Method == http.MethodGet:
			f.download(w, r, resource)
		case op == "/content/" && r.Method == http.MethodPut:
			f.upload(w, r, resource)
		case op == "/content/" && r.Method == http.MethodPatch && f.patch:
			f.setModTime(w, r, resource)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such route")
}

// validBucketName checks a bucket name against the S3 naming rules
var validBucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// serveBuckets serves the bucket endpoints, name is "" for the
// collection
func (f *fakeServer) serveBuckets(w http.ResponseWriter, r *http.Request, name string) {
	switch {
	case name == "" && r.Method == http.MethodGet:
		var buckets []string
		for b := range f.buckets {
			buckets = append(buckets, b)
		}
		sort.Strings(buckets)
		writeJSON(w, http.StatusOK, api.BucketList{Kind: api.KindBucketList, Buckets: buckets})
	case name == "" && r.Method == http.MethodPost:
		var req api.CreateBucket
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !validBucketName.MatchString(req.Name) {
			jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid bucket name")
			return
		}
		status := http.StatusCreated
		if f.buckets[req.Name] {
			status = http.StatusOK
		}
		f.buckets[req.Name] = true
		writeJSON(w, status, api.Bucket{Kind: api.KindBucket, Name: req.Name})
	case name == "":
		w.WriteHeader(http.StatusMethodNotAllowed)
	case !f.buckets[name]:
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such bucket")
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		writeJSON(w, http.StatusOK, api.Bucket{Kind: api.KindBucket, Name: name})
	case r.Method == http.MethodDelete:
		// only files make a bucket not empty - empty folders are
		// deleted with it
		prefix := name + "/"
		for p := range f.files {
			if strings.HasPrefix(p, prefix) {
				jsonError(w, http.StatusConflict, "CONFLICT", "bucket not empty")
				return
			}
		}
		for p := range f.folders {
			if strings.HasPrefix(p, prefix) {
				delete(f.folders, p)
			}
		}
		delete(f.buckets, name)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// formatTime formats t as the server does
func (f *fakeServer) formatTime(t time.Time) string {
	if !f.nanos {
		t = t.Truncate(time.Second)
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// fileResource makes the FileResource for the file at p
func (f *fakeServer) fileResource(p string, file *fakeFile) api.File {
	parent, name := "", p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		parent, name = p[:i+1], p[i+1:]
	}
	sum := md5.Sum(file.data)
	md5sum := hex.EncodeToString(sum[:])
	if file.pending {
		md5sum = "PENDING"
	}
	return api.File{
		Kind:         api.KindFile,
		ID:           strconv.Itoa(file.id),
		Name:         name,
		Path:         p,
		ParentPath:   parent,
		Size:         int64(len(file.data)),
		MimeType:     mimeType(name),
		MD5Checksum:  md5sum,
		CreatedTime:  f.formatTime(file.created),
		ModifiedTime: f.formatTime(file.mtime),
		CreatedAt:    file.created.Unix(),
		UpdatedAt:    file.mtime.Unix(),
	}
}

// mimeType guesses the MIME type from the extension as the server does
func mimeType(name string) string {
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		return t
	}
	return "application/octet-stream"
}

// folderResource makes the FolderResource for the prefix p ending in /
func folderResource(p string) api.Folder {
	trimmed := strings.TrimSuffix(p, "/")
	parent, name := "", trimmed
	if i := strings.LastIndex(trimmed, "/"); i >= 0 {
		parent, name = trimmed[:i+1], trimmed[i+1:]
	}
	return api.Folder{Kind: api.KindFolder, Name: name, Path: p, ParentPath: parent}
}

// folderExists returns true if prefix (ending in /) is a folder
func (f *fakeServer) folderExists(prefix string) bool {
	if f.folders[prefix] {
		return true
	}
	for p := range f.files {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	// the root of a bucket always exists
	return strings.Count(prefix, "/") == 1 && f.buckets[strings.TrimSuffix(prefix, "/")]
}

// addParents records the parent folders of p in direct-fs mode
func (f *fakeServer) addParents(p string) {
	if f.packed {
		return
	}
	for i := strings.Index(p, "/"); i >= 0 && i < len(p)-1; {
		next := strings.Index(p[i+1:], "/")
		if next < 0 {
			break
		}
		i += next + 1
		f.folders[p[:i+1]] = true
	}
}

func (f *fakeServer) list(w http.ResponseWriter, r *http.Request, resource string) {
	prefix := resource
	if !strings.HasSuffix(prefix, "/") {
		if file, ok := f.files[resource]; ok && strings.Contains(resource, "/") {
			writeJSON(w, http.StatusOK, f.fileResource(resource, file))
			return
		}
		prefix += "/"
	}
	if !f.folderExists(prefix) {
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "not found")
		return
	}
	recursive := f.recursive && r.URL.Query().Get("recursive") == "true"
	type entry struct {
		name   string
		file   *api.File
		folder *api.Folder
	}
	var entries []entry
	seen := map[string]bool{}
	addFolder := func(p string) {
		if !seen[p] {
			seen[p] = true
			folder := folderResource(p)
			entries = append(entries, entry{name: p, folder: &folder})
		}
	}
	for p, file := range f.files {
		rest, ok := strings.CutPrefix(p, prefix)
		if !ok {
			continue
		}
		if i := strings.Index(rest, "/"); i >= 0 && !recursive {
			addFolder(prefix + rest[:i+1])
			continue
		}
		info := f.fileResource(p, file)
		entries = append(entries, entry{name: p, file: &info})
	}
	if !recursive {
		for p := range f.folders {
			rest, ok := strings.CutPrefix(p, prefix)
			if ok && rest != "" {
				i := strings.Index(rest, "/")
				addFolder(prefix + rest[:i+1])
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	pageSize := 100
	if s := r.URL.Query().Get("pageSize"); s != "" {
		pageSize, _ = strconv.Atoi(s)
	}
	pageSize = max(1, min(pageSize, f.maxPage))
	offset := 0
	if token := r.URL.Query().Get("pageToken"); token != "" {
		b, err := base64.RawURLEncoding.DecodeString(token)
		if err == nil {
			offset, err = strconv.Atoi(string(b))
		}
		if err != nil {
			jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad pageToken")
			return
		}
	}
	end := min(offset+pageSize, len(entries))
	result := api.List{
		Kind:     api.KindFileList,
		Path:     prefix,
		Files:    []api.File{},
		Folders:  []api.Folder{},
		PageSize: pageSize,
		Offset:   offset,
		HasMore:  end < len(entries),
	}
	if recursive && f.recursiveFolders {
		result.Recursive = true
		if offset == 0 {
			result.Folders = f.foldersBelow(prefix)
		}
	}
	for _, e := range entries[min(offset, end):end] {
		if e.file != nil {
			result.Files = append(result.Files, *e.file)
		} else {
			result.Folders = append(result.Folders, *e.folder)
		}
	}
	if result.HasMore {
		result.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	writeJSON(w, http.StatusOK, result)
}

// foldersBelow returns every folder below prefix at any depth
func (f *fakeServer) foldersBelow(prefix string) []api.Folder {
	found := map[string]bool{}
	add := func(p string) {
		rest, ok := strings.CutPrefix(p, prefix)
		if !ok {
			return
		}
		for i := strings.Index(rest, "/"); i >= 0; {
			found[prefix+rest[:i+1]] = true
			next := strings.Index(rest[i+1:], "/")
			if next < 0 {
				break
			}
			i += next + 1
		}
	}
	for p := range f.files {
		add(p)
	}
	for p := range f.folders {
		add(p)
	}
	folders := []api.Folder{}
	for p := range found {
		folders = append(folders, folderResource(p))
	}
	sort.Slice(folders, func(i, j int) bool { return folders[i].Path < folders[j].Path })
	return folders
}

func (f *fakeServer) download(w http.ResponseWriter, r *http.Request, resource string) {
	file, ok := f.files[resource]
	if !ok {
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such file")
		return
	}
	sum := md5.Sum(file.data)
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
	http.ServeContent(w, r, "", file.mtime, bytes.NewReader(file.data))
}

// parseMtime parses the mtime parameter as the server does
func (f *fakeServer) parseMtime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if f.nanos {
		t, err := swift.FloatStringToTime(s)
		return t, err == nil
	}
	secs, err := strconv.ParseInt(s, 10, 64)
	return time.Unix(secs, 0), err == nil
}

func (f *fakeServer) upload(w http.ResponseWriter, r *http.Request, resource string) {
	if !strings.Contains(resource, "/") || strings.HasSuffix(resource, "/") {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad key")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	sum := md5.Sum(data)
	if want := r.URL.Query().Get("md5"); want != "" && want != hex.EncodeToString(sum[:]) {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "md5 mismatch")
		return
	}
	if want := r.Header.Get("Content-MD5"); want != "" && want != base64.StdEncoding.EncodeToString(sum[:]) {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "Content-MD5 mismatch")
		return
	}
	now := f.now()
	mtime, ok := f.parseMtime(r.URL.Query().Get("mtime"))
	if !ok {
		// an unparsable mtime is replaced by the upload time
		mtime = now
	}
	status := http.StatusOK
	file, exists := f.files[resource]
	if !exists {
		status = http.StatusCreated
		f.nextID++
		file = &fakeFile{id: f.nextID, created: now}
		f.files[resource] = file
	}
	file.data, file.mtime = data, mtime
	f.addParents(resource)
	writeJSON(w, status, f.fileResource(resource, file))
}

func (f *fakeServer) setModTime(w http.ResponseWriter, r *http.Request, resource string) {
	file, ok := f.files[resource]
	if !ok {
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such file")
		return
	}
	var req api.SetModTime
	err := json.NewDecoder(r.Body).Decode(&req)
	if err == nil {
		file.mtime, err = time.Parse(time.RFC3339Nano, req.ModifiedTime)
	}
	if err != nil {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, f.fileResource(resource, file))
}

func (f *fakeServer) createFolder(w http.ResponseWriter, r *http.Request) {
	if f.packed {
		jsonError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "packed-volume mode can't hold empty folders")
		return
	}
	var req api.CreateFolder
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !validPath(req.Path) || !strings.HasSuffix(req.Path, "/") {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad path")
		return
	}
	bucket, _, _ := strings.Cut(req.Path, "/")
	if !f.buckets[bucket] {
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such bucket")
		return
	}
	status := http.StatusCreated
	if f.folders[req.Path] {
		status = http.StatusOK
	}
	f.folders[req.Path] = true
	f.addParents(req.Path)
	writeJSON(w, status, folderResource(req.Path))
}

func (f *fakeServer) moveCopy(w http.ResponseWriter, r *http.Request, move bool) {
	var req api.MoveCopy
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !validPath(req.From) || !validPath(req.To) {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad request")
		return
	}
	status := http.StatusCreated
	if move {
		status = http.StatusOK
	}
	if bucket, _, _ := strings.Cut(req.To, "/"); !f.buckets[bucket] {
		jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such bucket")
		return
	}
	notFound := func() {
		if f.missing404 {
			jsonError(w, http.StatusNotFound, "NOT_FOUND", "no such source")
		} else {
			writeJSON(w, status, api.Folder{Kind: api.KindFolder})
		}
	}
	if !strings.HasSuffix(req.From, "/") {
		file, ok := f.files[req.From]
		if !ok {
			notFound()
			return
		}
		dst := &fakeFile{id: file.id, data: file.data, mtime: file.mtime, created: file.created}
		if move {
			delete(f.files, req.From)
		} else {
			f.nextID++
			dst.id = f.nextID
		}
		f.files[req.To] = dst
		f.addParents(req.To)
		writeJSON(w, status, f.fileResource(req.To, dst))
		return
	}
	if !strings.HasSuffix(req.To, "/") || strings.HasPrefix(req.To, req.From) {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad destination")
		return
	}
	if !f.folderExists(req.From) {
		notFound()
		return
	}
	for p, file := range f.files {
		if rest, ok := strings.CutPrefix(p, req.From); ok {
			dst := &fakeFile{id: file.id, data: file.data, mtime: file.mtime, created: file.created}
			if move {
				delete(f.files, p)
			} else {
				f.nextID++
				dst.id = f.nextID
			}
			f.files[req.To+rest] = dst
			f.addParents(req.To + rest)
		}
	}
	for p := range f.folders {
		if rest, ok := strings.CutPrefix(p, req.From); ok {
			if move {
				delete(f.folders, p)
			}
			f.folders[req.To+rest] = true
			f.addParents(req.To + rest)
		}
	}
	if move {
		// the bucket root itself stays
		if strings.Count(req.From, "/") == 1 {
			f.buckets[strings.TrimSuffix(req.From, "/")] = true
		}
	}
	if !f.packed {
		f.folders[req.To] = true
		f.addParents(req.To)
	}
	writeJSON(w, status, folderResource(req.To))
}

func (f *fakeServer) delete(w http.ResponseWriter, r *http.Request) {
	var req api.Delete
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "bad request")
		return
	}
	var n int64
	for _, target := range req.Targets {
		if !validPath(target) {
			jsonError(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("bad target %q", target))
			return
		}
		if !strings.HasSuffix(target, "/") {
			if _, ok := f.files[target]; ok {
				delete(f.files, target)
				n++
			}
			continue
		}
		for p := range f.files {
			if strings.HasPrefix(p, target) {
				delete(f.files, p)
				n++
			}
		}
		for p := range f.folders {
			if strings.HasPrefix(p, target) {
				delete(f.folders, p)
			}
		}
	}
	writeJSON(w, http.StatusOK, api.DeleteResult{Kind: "kamplexfs#deleteResult", Targets: req.Targets, DeletedCount: n})
}
