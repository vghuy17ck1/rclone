// Package kamplexfs provides an interface to the KamPlexFS JSON REST API
package kamplexfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/backend/kamplexfs/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/list"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/semaphore"
)

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential
	defaultRoute  = "/api/v1/files"
	// iat is backdated by this much to allow for clock skew
	clockSkew = 30 * time.Second
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "kamplexfs",
		Description: "KamPlexFS",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     "URL of the KamPlexFS server.\n\nE.g. \"http://localhost:8080\".",
			Required: true,
		}, {
			Name: "jwt_secret",
			Help: `The server's JWT secret.

This is the jwt_secret from the [security] section of the server's
config. It is used to sign short lived tokens for each request.

Leave blank to use a fixed token instead.`,
			IsPassword: true,
		}, {
			Name: "token",
			Help: `A fixed JWT to use instead of the JWT secret.

rclone can't renew this token, so once it expires requests fail.`,
			Sensitive: true,
			Advanced:  true,
		}, {
			Name: "jwt_ttl",
			Help: `Lifetime of the tokens signed with the JWT secret.

This must not be longer than the server's jwt_max_ttl_seconds, which
defaults to 24h.`,
			Default:  fs.Duration(time.Hour),
			Advanced: true,
		}, {
			Name: "allowed_prefixes",
			Help: `Path prefixes to restrict the signed tokens to.

A comma separated list which is put in the allowed_prefixes claim of
the tokens. Leave blank for no restriction.`,
			Default:  fs.CommaSepList{},
			Advanced: true,
		}, {
			Name: "allowed_methods",
			Help: `HTTP methods to restrict the signed tokens to.

A comma separated list which is put in the allowed_methods claim of
the tokens. Leave blank for no restriction.`,
			Default:  fs.CommaSepList{},
			Advanced: true,
		}, {
			Name:     "route",
			Help:     "The route the JSON API is mounted on.\n\nThe bucket endpoints are expected next to it, e.g. /api/v1/buckets\nfor /api/v1/files.",
			Default:  defaultRoute,
			Advanced: true,
		}, {
			Name:     "list_chunk",
			Help:     "Size of listing chunk.\n\nThe server may return fewer.",
			Default:  1000,
			Advanced: true,
		}, {
			Name: "blake3_concurrency",
			Help: `Maximum number of BLAKE3 hashes computed at once.

When the server computes BLAKE3 digests, rclone hashes the data of
each upload as it sends it and fails the upload if the digest the
server returns is different. This limits how many uploads are hashed
at the same moment, and so the CPU used for it.

0 means the number of CPUs.`,
			Default:  0,
			Advanced: true,
		}, {
			Name: "hashes",
			Help: `Hashes to use instead of the ones the server lists.

A comma separated list of md5 and blake3, or none for no hashes.
Leave blank to use the hashes the server lists with its buckets.

For example md5 turns BLAKE3 off, saving the hashing of uploads, and
blake3 makes rclone compare files by BLAKE3 only. Setting it also
saves the request which finds out the server's hashes, which tokens
restricted with allowed_prefixes can't make. Only list hashes the
server computes, as files have no hash otherwise.`,
			Default:  fs.CommaSepList{},
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// The same as the s3 backend so the objects have the
			// same names through both.
			Default: encoder.EncodeInvalidUtf8 |
				encoder.EncodeSlash |
				encoder.EncodeDot,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	URL               string               `config:"url"`
	JWTSecret         string               `config:"jwt_secret"`
	Token             string               `config:"token"`
	JWTTTL            fs.Duration          `config:"jwt_ttl"`
	AllowedPrefixes   fs.CommaSepList      `config:"allowed_prefixes"`
	AllowedMethods    fs.CommaSepList      `config:"allowed_methods"`
	Route             string               `config:"route"`
	ListChunk         int                  `config:"list_chunk"`
	Blake3Concurrency int                  `config:"blake3_concurrency"`
	Hashes            fs.CommaSepList      `config:"hashes"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a remote KamPlexFS server
type Fs struct {
	name          string        // name of this remote
	root          string        // the path we are working on
	opt           Options       // parsed options
	features      *fs.Features  // optional features
	srv           *rest.Client  // the connection to the server
	pacer         *fs.Pacer     // pacer for API calls
	tokens        *tokenSource  // JWTs for the requests
	rootBucket    string        // bucket part of root (if any)
	rootDirectory string        // directory part of root (if any)
	bucketsURL    string        // URL of the bucket endpoints
	cache         *bucket.Cache // cache for bucket creation status
	server        *serverState  // what has been learnt about the server
	noFoldersOnce sync.Once

	ctx        context.Context     // for the request in Hashes
	hashesOnce sync.Once           // Hashes has asked the server
	hashSem    *semaphore.Weighted // limits the BLAKE3 hashing of uploads
	hashes     hash.Set            // hashes from the options, 0 if not set
}

// serverState is what has been learnt about a server from its
// responses, shared by all the Fs talking to it.
type serverState struct {
	mtimeNanos   atomic.Bool  // set if the server stores sub-second mtimes
	noSetModTime atomic.Bool  // set if the server can't set mtimes
	noRecursive  atomic.Bool  // set if the server can't list recursively
	noFolders    atomic.Bool  // set if the server can't store empty folders
	bucketAPI    atomic.Int32 // whether the server has bucket endpoints
	// whether recursive listings have every folder in the first page
	recursiveFolders atomic.Int32
	hashesMu         sync.Mutex
	hashes           hash.Set // hashes the server has
	hashesKnown      bool     // set if hashes has been read
}

// Values of the serverState fields which are unknown until the
// server has been asked
const (
	stateUnknown int32 = iota
	stateYes
	stateNo
)

// servers holds the serverState for each server keyed by its base URL
var servers sync.Map

// Object describes a KamPlexFS object
type Object struct {
	fs       *Fs       // what this object is part of
	remote   string    // the remote path
	id       string    // ID of the object
	size     int64     // size of the object
	modTime  time.Time // modification time of the object
	md5      string    // MD5 of the object or "" if unknown
	blake3   string    // BLAKE3 of the object or "" if unknown
	mimeType string    // MIME type of the object
}

// ------------------------------------------------------------

// tokenSource makes the JWTs used to authorize requests
type tokenSource struct {
	mu       sync.Mutex
	secret   []byte           // HMAC key, nil if not minting tokens
	static   string           // fixed token, if set
	ttl      time.Duration    // lifetime of minted tokens
	prefixes []string         // allowed_prefixes claim if set
	methods  []string         // allowed_methods claim if set
	now      func() time.Time // for testing
	token    string           // current token
	expiry   time.Time        // when token expires
}

// canRefresh returns true if new tokens can be minted
func (ts *tokenSource) canRefresh() bool {
	return ts.static == "" && len(ts.secret) > 0
}

// margin is how long before expiry a token is replaced
func (ts *tokenSource) margin() time.Duration {
	return max(60*time.Second, ts.ttl/10)
}

// Token returns a token which isn't about to expire or "" if there
// is no authorization.
func (ts *tokenSource) Token() (string, error) {
	if !ts.canRefresh() {
		return ts.static, nil
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	now := ts.now()
	if ts.token != "" && now.Add(ts.margin()).Before(ts.expiry) {
		return ts.token, nil
	}
	expiry := now.Add(ts.ttl)
	claims := jwt.MapClaims{
		"iat": now.Add(-clockSkew).Unix(),
		"exp": expiry.Unix(),
	}
	if len(ts.prefixes) > 0 {
		claims["allowed_prefixes"] = ts.prefixes
	}
	if len(ts.methods) > 0 {
		claims["allowed_methods"] = ts.methods
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(ts.secret)
	if err != nil {
		return "", fmt.Errorf("failed to sign JWT: %w", err)
	}
	ts.token, ts.expiry = token, expiry
	return token, nil
}

// Invalidate discards token so the next call to Token makes a new
// one, unless it has been replaced already.
func (ts *tokenSource) Invalidate(token string) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.token == token {
		ts.token = ""
	}
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.rootBucket == "" {
		return "KamPlexFS root"
	}
	if f.rootDirectory == "" {
		return fmt.Sprintf("KamPlexFS bucket %s", f.rootBucket)
	}
	return fmt.Sprintf("KamPlexFS bucket %s path %s", f.rootBucket, f.rootDirectory)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried. It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// errorHandler parses a non 2xx response into an *api.Error
func errorHandler(resp *http.Response) error {
	apiErr := &api.Error{StatusCode: resp.StatusCode}
	body, err := rest.ReadBody(resp)
	if err != nil {
		apiErr.Text = fmt.Sprintf("failed to read error body: %v", err)
		return apiErr
	}
	if json.Unmarshal(body, apiErr) != nil || (apiErr.Details.Status == "" && apiErr.Details.Message == "") {
		apiErr.Text = strings.TrimSpace(string(body))
		if apiErr.Text == "" {
			apiErr.Text = resp.Status
		}
	}
	return apiErr
}

// statusCode returns the HTTP status of an *api.Error or 0
func statusCode(err error) int {
	var apiErr *api.Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	return 0
}

// call calls fn with the Authorization header set in opts
//
// If the server rejects the token then a new token is made and the
// call retried once, unless noRetry is set when the call isn't
// retried at all.
func (f *Fs) call(ctx context.Context, opts *rest.Opts, noRetry bool, fn func() (*http.Response, error)) (resp *http.Response, err error) {
	refreshed := false
	do := func() (bool, error) {
		token, err := f.tokens.Token()
		if err != nil {
			return false, err
		}
		if token != "" {
			if opts.ExtraHeaders == nil {
				opts.ExtraHeaders = map[string]string{}
			}
			opts.ExtraHeaders["Authorization"] = "Bearer " + token
		}
		resp, err = fn()
		if statusCode(err) == http.StatusUnauthorized && f.tokens.canRefresh() {
			f.tokens.Invalidate(token)
			if !refreshed {
				refreshed = true
				fs.Debugf(f, "Token rejected, retrying with a new one: %v", err)
				return true, err
			}
		}
		return shouldRetry(ctx, resp, err)
	}
	if noRetry {
		err = f.pacer.CallNoRetry(do)
	} else {
		err = f.pacer.Call(do)
	}
	return resp, err
}

// callJSON calls the API with JSON request and response bodies
func (f *Fs) callJSON(ctx context.Context, opts *rest.Opts, request, response any) (*http.Response, error) {
	return f.call(ctx, opts, false, func() (*http.Response, error) {
		return f.srv.CallJSON(ctx, opts, request, response)
	})
}

// parsePath parses a remote path
func parsePath(path string) (root string) {
	return strings.Trim(path, "/")
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = parsePath(root)
	f.rootBucket, f.rootDirectory = bucket.Split(f.root)
}

// split returns bucket and bucketPath from the rootRelativePath
// relative to f.root, encoded for the server
func (f *Fs) split(rootRelativePath string) (bucketName, bucketPath string) {
	bucketName, bucketPath = bucket.Split(bucket.Join(f.root, rootRelativePath))
	return f.opt.Enc.FromStandardName(bucketName), f.opt.Enc.FromStandardPath(bucketPath)
}

// split returns bucket and bucketPath of the object
func (o *Object) split() (bucket, bucketPath string) {
	return o.fs.split(o.remote)
}

// serverPath joins bucket and key into a server path
func serverPath(bucket, key string) string {
	if key == "" {
		return bucket
	}
	return bucket + "/" + key
}

// dirPath returns the server path of a directory, ending in "/"
func dirPath(bucket, directory string) string {
	if directory == "" {
		return bucket + "/"
	}
	return bucket + "/" + directory + "/"
}

// escape URL encodes each segment of a server path
func escape(p string) string {
	return rest.URLPathEscapeAll(p)
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.URL == "" {
		return nil, errors.New("kamplexfs: url is required")
	}
	if opt.JWTTTL <= 0 {
		return nil, errors.New("kamplexfs: jwt_ttl must be positive")
	}
	if opt.ListChunk <= 0 {
		opt.ListChunk = 1000
	}
	route := "/" + strings.Trim(opt.Route, "/")
	if route == "/" {
		route = ""
	}
	ts := &tokenSource{
		static:   opt.Token,
		ttl:      time.Duration(opt.JWTTTL),
		prefixes: opt.AllowedPrefixes,
		methods:  opt.AllowedMethods,
		now:      time.Now,
	}
	if opt.Token == "" && opt.JWTSecret != "" {
		secret, err := obscure.Reveal(opt.JWTSecret)
		if err != nil {
			return nil, fmt.Errorf("kamplexfs: couldn't decrypt jwt_secret: %w", err)
		}
		ts.secret = []byte(secret)
	}
	if !ts.canRefresh() && ts.static == "" {
		fs.Debugf(nil, "kamplexfs: no jwt_secret or token so not sending authorization")
	}

	rootURL := strings.TrimSuffix(opt.URL, "/") + route
	server, _ := servers.LoadOrStore(rootURL, &serverState{})
	f := &Fs{
		name:   name,
		opt:    *opt,
		ctx:    ctx,
		tokens: ts,
		server: server.(*serverState),
		// The bucket endpoints are mounted next to the file ones,
		// e.g. /api/v1/buckets for /api/v1/files
		bucketsURL: strings.TrimSuffix(opt.URL, "/") + path.Join("/", path.Dir(route), "buckets"),
		cache:      bucket.NewCache(),
		srv:        rest.NewClient(fshttp.NewClient(ctx)).SetRoot(rootURL).SetErrorHandler(errorHandler),
		pacer:      fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
	}
	f.setRoot(root)
	f.features = (&fs.Features{
		ReadMimeType:            true,
		CanHaveEmptyDirectories: !f.server.noFolders.Load(),
		BucketBased:             true,
		BucketBasedRootOK:       true,
	}).Fill(ctx, f)
	workers := opt.Blake3Concurrency
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	f.hashSem = semaphore.NewWeighted(int64(workers))
	if len(opt.Hashes) > 0 {
		f.hashes, err = parseHashes(opt.Hashes)
		if err != nil {
			return nil, err
		}
	}

	// Check to see if the root is actually an existing file
	if f.rootBucket != "" && f.rootDirectory != "" && !strings.HasSuffix(root, "/") {
		oldRoot := f.root
		newRoot, leaf := path.Split(oldRoot)
		f.setRoot(newRoot)
		_, err := f.NewObject(ctx, leaf)
		if err == nil {
			return f, fs.ErrorIsFile
		}
		f.setRoot(oldRoot)
	}
	return f, nil
}

// newObject makes an Object from a FileResource
func (f *Fs) newObject(remote string, info *api.File) (*Object, error) {
	o := &Object{fs: f, remote: remote}
	return o, o.setMetaData(info)
}

// stat reads the resource at bucket/key which may be a file or a
// folder listing
func (f *Fs) stat(ctx context.Context, bucket, key string) (*api.Item, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/list/" + escape(serverPath(bucket, key)),
	}
	var item api.Item
	_, err := f.callJSON(ctx, &opts, nil, &item)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	bucket, key := f.split(remote)
	if bucket == "" || key == "" {
		return nil, fs.ErrorObjectNotFound
	}
	item, err := f.stat(ctx, bucket, key)
	if err != nil {
		if statusCode(err) == http.StatusNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	if item.Kind != api.KindFile {
		return nil, fs.ErrorIsDir
	}
	return f.newObject(remote, &item.File)
}

// errEndList stops a listing early
var errEndList = errors.New("end list")

// errWalkInstead is returned by a recursive listing which would leave
// out empty folders
var errWalkInstead = errors.New("recursive listing has no folders")

// listPages lists the folder at bucket/directory calling fnFile and
// fnFolder for each entry, following the pages of the listing.
//
// It returns fs.ErrorDirNotFound if the folder doesn't exist, and
// errWalkInstead before calling anything if a recursive listing leaves
// out folders which may be empty.
func (f *Fs) listPages(ctx context.Context, bucket, directory string, recursive bool, fnFile func(*api.File) error, fnFolder func(*api.Folder) error) error {
	p := "/list/" + escape(bucket)
	if directory != "" {
		p += "/" + escape(directory) + "/"
	}
	pageToken := ""
	for {
		opts := rest.Opts{
			Method:     "GET",
			Path:       p,
			Parameters: url.Values{"pageSize": {strconv.Itoa(f.opt.ListChunk)}},
		}
		if pageToken != "" {
			opts.Parameters.Set("pageToken", pageToken)
		}
		if recursive {
			opts.Parameters.Set("recursive", "true")
		}
		var result api.List
		_, err := f.callJSON(ctx, &opts, nil, &result)
		if err != nil {
			if statusCode(err) == http.StatusNotFound {
				return fs.ErrorDirNotFound
			}
			return fmt.Errorf("list failed: %w", err)
		}
		if recursive && pageToken == "" {
			f.noteRecursiveFolders(result.Recursive)
			if !result.Recursive && !f.server.noFolders.Load() {
				return errWalkInstead
			}
		}
		for i := range result.Folders {
			err = fnFolder(&result.Folders[i])
			if err != nil {
				return err
			}
		}
		for i := range result.Files {
			err = fnFile(&result.Files[i])
			if err != nil {
				return err
			}
		}
		if !result.HasMore || result.NextPageToken == "" {
			return nil
		}
		pageToken = result.NextPageToken
	}
}

// listDir lists bucket/directory which is dir relative to the root,
// calling callback for each entry.
//
// If recursive is set everything below the directory is listed.
func (f *Fs) listDir(ctx context.Context, bucket, directory, dir string, recursive bool, callback func(fs.DirEntry) error) error {
	prefix := dirPath(bucket, directory)
	// remote returns the remote of the server path p
	remote := func(p, name string) string {
		rel, ok := strings.CutPrefix(strings.TrimSuffix(p, "/"), prefix)
		if !ok || rel == "" {
			rel = name
		}
		return path.Join(dir, f.opt.Enc.ToStandardPath(rel))
	}
	return f.listPages(ctx, bucket, directory, recursive && !f.server.noRecursive.Load(), func(info *api.File) error {
		o, err := f.newObject(remote(info.Path, info.Name), info)
		if err != nil {
			return err
		}
		return callback(o)
	}, func(info *api.Folder) error {
		dirRemote := remote(info.Path, info.Name)
		err := callback(fs.NewDir(dirRemote, time.Time{}))
		if err != nil || !recursive || f.server.recursiveFolders.Load() == stateYes {
			return err
		}
		// A server which can list recursively without saying so
		// returns no folders
		if !f.server.noRecursive.Swap(true) {
			fs.Debugf(f, "Server can't list recursively - walking the folders instead")
		}
		_, subDirectory := f.split(dirRemote)
		return f.listDir(ctx, bucket, subDirectory, dirRemote, true, callback)
	})
}

// noBucketAPI returns true if err from a bucket endpoint shows the
// server doesn't have them, recording it if so.
func (f *Fs) noBucketAPI(err error) bool {
	switch statusCode(err) {
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		if f.server.bucketAPI.Swap(stateNo) != stateNo {
			fs.Debugf(f, "Server has no bucket endpoints so buckets must already exist: %v", err)
		}
		return true
	}
	return false
}

// callBuckets calls the bucket endpoint for name, or the collection
// of buckets if name is "".
func (f *Fs) callBuckets(ctx context.Context, method, name string, request, response any) error {
	opts := rest.Opts{
		Method:  method,
		RootURL: f.bucketsURL,
	}
	if name != "" {
		opts.Path = "/" + escape(name)
	}
	_, err := f.callJSON(ctx, &opts, request, response)
	return err
}

// hasBucketAPI returns true if the server has the bucket endpoints,
// listing the buckets to find out if it isn't known yet.
func (f *Fs) hasBucketAPI(ctx context.Context) (bool, error) {
	switch f.server.bucketAPI.Load() {
	case stateYes:
		return true, nil
	case stateNo:
		return false, nil
	}
	_, err := f.listBuckets(ctx)
	if statusCode(err) == http.StatusForbidden {
		// Listing the buckets needs an unscoped token so the
		// endpoint is there but this token can't use it
		f.server.bucketAPI.Store(stateYes)
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return f.server.bucketAPI.Load() == stateYes, nil
}

// noteRecursiveFolders records whether recursive listings have every
// folder, as marked on their first page
func (f *Fs) noteRecursiveFolders(marked bool) {
	state := stateNo
	if marked {
		state = stateYes
	}
	if f.server.recursiveFolders.Swap(state) != state {
		fs.Debugf(f, "Recursive listings have every folder: %v", marked)
	}
}

// listBuckets lists the buckets
func (f *Fs) listBuckets(ctx context.Context) (entries fs.DirEntries, err error) {
	var result api.BucketList
	if f.server.bucketAPI.Load() != stateNo {
		err = f.callBuckets(ctx, "GET", "", nil, &result)
		if err == nil {
			f.server.bucketAPI.Store(stateYes)
		} else if !f.noBucketAPI(err) {
			return nil, fmt.Errorf("failed to list buckets: %w", err)
		}
	}
	if f.server.bucketAPI.Load() == stateNo {
		opts := rest.Opts{
			Method: "GET",
			Path:   "/list",
		}
		_, err = f.callJSON(ctx, &opts, nil, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to list buckets: %w", err)
		}
	}
	f.setHashes(result.Hashes)
	for _, name := range result.Buckets {
		entries = append(entries, fs.NewDir(f.opt.Enc.ToStandardName(name), time.Time{}))
	}
	return entries, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	bucket, directory := f.split(dir)
	if bucket == "" {
		if directory != "" {
			return nil, fs.ErrorListBucketRequired
		}
		return f.listBuckets(ctx)
	}
	err = f.listDir(ctx, bucket, directory, dir, false, func(entry fs.DirEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// ListR lists the objects and directories of the Fs starting
// from dir recursively into out.
//
// dir should be "" to start from the root, and should not
// have trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
//
// It should call callback for each tranche of entries read.
// These need not be returned in any particular order.  If
// callback returns an error then the listing will stop
// immediately.
//
// Don't implement this unless you have a more efficient way
// of listing recursively than doing a directory traversal.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	if !f.server.noFolders.Load() && f.server.recursiveFolders.Load() == stateNo {
		return f.walkR(ctx, dir, callback)
	}
	err := f.listR(ctx, dir, callback)
	if err == errWalkInstead {
		return f.walkR(ctx, dir, callback)
	}
	return err
}

// walkR lists dir recursively by listing each folder in turn
//
// It is used when recursive listings leave out empty folders which
// the server can hold unless it is in packed-volume mode.
func (f *Fs) walkR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	ctx, ci := fs.AddConfig(ctx)
	ci.UseListR = false
	notFound := false
	err := walk.Walk(ctx, f, dir, true, -1, func(p string, entries fs.DirEntries, err error) error {
		if err == fs.ErrorDirNotFound && p == dir {
			notFound = true
			return nil
		}
		if err != nil {
			return err
		}
		return callback(entries)
	})
	if err == nil && notFound {
		return fs.ErrorDirNotFound
	}
	return err
}

// listR lists dir with recursive listings
//
// It returns errWalkInstead before calling callback if they would
// leave out empty folders.
func (f *Fs) listR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	bucket, directory := f.split(dir)
	helper := list.NewHelper(callback)
	if bucket == "" {
		if directory != "" {
			return fs.ErrorListBucketRequired
		}
		buckets, err := f.listBuckets(ctx)
		if err != nil {
			return err
		}
		for _, entry := range buckets {
			err = helper.Add(entry)
			if err != nil {
				return err
			}
			bucketRemote := entry.Remote()
			err = f.listDir(ctx, f.opt.Enc.FromStandardName(bucketRemote), "", bucketRemote, true, helper.Add)
			if err != nil {
				return err
			}
		}
	} else {
		err := f.listDir(ctx, bucket, directory, dir, true, helper.Add)
		if err != nil {
			return err
		}
	}
	return helper.Flush()
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned.
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// setNoFolders records that the server can't store empty folders
func (f *Fs) setNoFolders() {
	f.noFoldersOnce.Do(func() {
		fs.Debugf(f, "Server can't store empty folders (packed-volume mode)")
		f.server.noFolders.Store(true)
		f.features.CanHaveEmptyDirectories = false
	})
}

// makeBucket creates the bucket if it doesn't exist
//
// Servers without the bucket endpoints can't create buckets so they
// must exist already.
func (f *Fs) makeBucket(ctx context.Context, bucketName string) error {
	return f.cache.Create(bucketName, func() error {
		if f.server.bucketAPI.Load() == stateNo {
			return nil
		}
		err := f.callBuckets(ctx, "POST", "", &api.CreateBucket{Name: bucketName}, nil)
		if err == nil {
			f.server.bucketAPI.Store(stateYes)
			return nil
		}
		if f.noBucketAPI(err) {
			return nil
		}
		if statusCode(err) == http.StatusForbidden {
			// A restricted token may not be allowed to create
			// buckets, or even to look at them, but the bucket
			// may exist anyway so carry on unless it definitely
			// doesn't.
			checkErr := f.callBuckets(ctx, "GET", bucketName, nil, nil)
			if statusCode(checkErr) != http.StatusNotFound {
				fs.Debugf(f, "Not allowed to create bucket %q, assuming it exists: %v", bucketName, err)
				return nil
			}
		}
		return fmt.Errorf("failed to create bucket %q: %w", bucketName, err)
	}, nil)
}

// Mkdir creates the folder if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	bucket, directory := f.split(dir)
	if bucket == "" {
		return nil
	}
	err := f.makeBucket(ctx, bucket)
	if err != nil || directory == "" {
		return err
	}
	if f.server.noFolders.Load() {
		f.setNoFolders()
		return nil
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/folders",
	}
	req := api.CreateFolder{Path: dirPath(bucket, directory)}
	var info api.Folder
	_, err = f.callJSON(ctx, &opts, &req, &info)
	if statusCode(err) == http.StatusNotImplemented {
		f.setNoFolders()
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to create folder: %w", err)
	}
	return nil
}

// isEmpty checks the folder bucket/directory is empty
//
// It returns fs.ErrorDirNotFound if it doesn't exist.
func (f *Fs) isEmpty(ctx context.Context, bucket, directory string) (bool, error) {
	empty := true
	found := func() error {
		empty = false
		return errEndList
	}
	err := f.listPages(ctx, bucket, directory, false, func(*api.File) error {
		return found()
	}, func(*api.Folder) error {
		return found()
	})
	if err != nil && err != errEndList {
		return false, err
	}
	return empty, nil
}

// removeBucket deletes the bucket
//
// It returns fs.ErrorDirNotFound if it doesn't exist,
// fs.ErrorDirectoryNotEmpty if it isn't empty and errNoBucketAPI if
// the server has no bucket endpoints.
func (f *Fs) removeBucket(ctx context.Context, bucketName string) error {
	err := f.callBuckets(ctx, "DELETE", bucketName, nil, nil)
	switch statusCode(err) {
	case http.StatusConflict:
		return fs.ErrorDirectoryNotEmpty
	case http.StatusNotFound:
		// A server without the bucket endpoints returns 404 too
		ok, checkErr := f.hasBucketAPI(ctx)
		if checkErr != nil {
			return checkErr
		}
		if ok {
			return fs.ErrorDirNotFound
		}
		return errNoBucketAPI
	case http.StatusMethodNotAllowed:
		f.noBucketAPI(err)
		return errNoBucketAPI
	}
	if err != nil {
		return fmt.Errorf("failed to delete bucket %q: %w", bucketName, err)
	}
	f.cache.MarkDeleted(bucketName)
	return nil
}

// errNoBucketAPI is returned by removeBucket if the server has no
// bucket endpoints
var errNoBucketAPI = errors.New("no bucket endpoints")

// Rmdir deletes the folder if it is empty
//
// Servers without the bucket endpoints can't delete buckets so they
// are left.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	bucket, directory := f.split(dir)
	if bucket == "" {
		return nil
	}
	if f.server.noFolders.Load() && directory != "" {
		// folders only exist while they hold objects
		return nil
	}
	// The server deletes a bucket holding only empty folders along
	// with them so this checks for folders too.
	empty, err := f.isEmpty(ctx, bucket, directory)
	if err != nil {
		return err
	}
	if !empty {
		return fs.ErrorDirectoryNotEmpty
	}
	if directory == "" {
		if f.server.bucketAPI.Load() == stateNo {
			return nil
		}
		err = f.removeBucket(ctx, bucket)
		if err == errNoBucketAPI {
			return nil
		}
		return err
	}
	// FIXME an object uploaded between the check and the delete is deleted too
	_, err = f.delete(ctx, dirPath(bucket, directory))
	return err
}

// delete deletes target, returning the number of objects deleted
//
// A target ending in "/" deletes everything under that prefix.
func (f *Fs) delete(ctx context.Context, target string) (int64, error) {
	opts := rest.Opts{
		Method: "DELETE",
		Path:   "",
	}
	req := api.Delete{Targets: []string{target}}
	var result api.DeleteResult
	_, err := f.callJSON(ctx, &opts, &req, &result)
	if err != nil {
		return 0, fmt.Errorf("failed to delete %q: %w", target, err)
	}
	return result.DeletedCount, nil
}

// Purge deletes all the files in the directory
func (f *Fs) Purge(ctx context.Context, dir string) error {
	bucket, directory := f.split(dir)
	if bucket == "" {
		return fs.ErrorCantPurge
	}
	n, err := f.delete(ctx, dirPath(bucket, directory))
	if err != nil {
		return err
	}
	if directory == "" && f.server.bucketAPI.Load() != stateNo {
		err = f.removeBucket(ctx, bucket)
		if err != errNoBucketAPI {
			return err
		}
	}
	if n == 0 {
		// Nothing was deleted so check the directory existed
		_, err = f.isEmpty(ctx, bucket, directory)
		if err == fs.ErrorDirNotFound {
			return err
		}
	}
	return nil
}

// Precision returns the precision of this Fs
//
// Whether the server stores sub-second mtimes is only discovered from
// the listings, and changing the precision part way through would
// make objects uploaded before that look modified, so it is always
// 1 second.
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// formatMtime formats t for the mtime parameter of an upload
//
// Servers without sub-second mtimes only accept whole seconds.
func (f *Fs) formatMtime(t time.Time) string {
	if f.server.mtimeNanos.Load() {
		return swift.TimeToFloatString(t)
	}
	return strconv.FormatInt(t.Unix(), 10)
}

// sameServer returns true if g talks to the same server as f
func (f *Fs) sameServer(g *Fs) bool {
	return f.server == g.server
}

// moveOrCopy moves or copies srcObj to remote with the endpoint at
// opPath, returning cantErr if the server can't do it.
func (f *Fs) moveOrCopy(ctx context.Context, opPath string, srcObj *Object, remote string, cantErr error) (fs.Object, error) {
	srcBucket, srcKey := srcObj.split()
	dstBucket, dstKey := f.split(remote)
	if dstKey == "" {
		return nil, cantErr
	}
	err := f.makeBucket(ctx, dstBucket)
	if err != nil {
		return nil, err
	}
	// Some servers report success when the source doesn't exist so
	// check it first.
	_, err = srcObj.fs.stat(ctx, srcBucket, srcKey)
	if statusCode(err) == http.StatusNotFound {
		return nil, fs.ErrorObjectNotFound
	} else if err != nil {
		return nil, err
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   opPath,
	}
	req := api.MoveCopy{
		From: serverPath(srcBucket, srcKey),
		To:   serverPath(dstBucket, dstKey),
	}
	var info api.Item
	_, err = f.callJSON(ctx, &opts, &req, &info)
	switch statusCode(err) {
	case http.StatusNotFound:
		return nil, fs.ErrorObjectNotFound
	case http.StatusBadRequest, http.StatusNotImplemented:
		fs.Debugf(srcObj, "Can't %s: %v", strings.TrimPrefix(opPath, "/"), err)
		return nil, cantErr
	}
	if err != nil {
		return nil, err
	}
	if info.Kind != api.KindFile {
		return nil, fs.ErrorObjectNotFound
	}
	return f.newObject(remote, &info.File)
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || !f.sameServer(srcObj.fs) {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	return f.moveOrCopy(ctx, "/copy", srcObj, remote, fs.ErrorCantCopy)
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok || !f.sameServer(srcObj.fs) {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	return f.moveOrCopy(ctx, "/move", srcObj, remote, fs.ErrorCantMove)
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || !f.sameServer(srcFs) {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	srcBucket, srcDirectory := srcFs.split(srcRemote)
	dstBucket, dstDirectory := f.split(dstRemote)
	if srcBucket == "" || dstBucket == "" {
		return fs.ErrorCantDirMove
	}
	_, err := f.isEmpty(ctx, dstBucket, dstDirectory)
	if err == nil {
		return fs.ErrorDirExists
	} else if err != fs.ErrorDirNotFound {
		return err
	}
	// Some servers report success when the source doesn't exist so
	// check it first.
	_, err = f.isEmpty(ctx, srcBucket, srcDirectory)
	if err != nil {
		return err
	}
	err = f.makeBucket(ctx, dstBucket)
	if err != nil {
		return err
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/move",
	}
	req := api.MoveCopy{
		From: dirPath(srcBucket, srcDirectory),
		To:   dirPath(dstBucket, dstDirectory),
	}
	var info api.Item
	_, err = f.callJSON(ctx, &opts, &req, &info)
	switch statusCode(err) {
	case http.StatusNotFound:
		return fs.ErrorDirNotFound
	case http.StatusBadRequest, http.StatusNotImplemented:
		fs.Debugf(srcFs, "Can't move directory: %v", err)
		return fs.ErrorCantDirMove
	}
	if err == nil && srcDirectory == "" {
		// Moving a whole bucket leaves it behind empty
		err = srcFs.Rmdir(ctx, srcRemote)
	}
	return err
}

// parseHashes parses the hashes option
func parseHashes(names []string) (hash.Set, error) {
	hashes := hash.Set(hash.None)
	for _, name := range names {
		var t hash.Type
		err := t.Set(strings.TrimSpace(name))
		if err != nil || (t != hash.None && t != hash.MD5 && t != hash.BLAKE3) {
			return hashes, fmt.Errorf("kamplexfs: hashes: %q isn't md5, blake3 or none", name)
		}
		hashes.Add(t)
	}
	return hashes, nil
}

// setHashes records the hashes of the server from a bucket listing
//
// Servers which don't say only have MD5.
func (f *Fs) setHashes(names []string) {
	hashes := hash.Set(hash.MD5)
	if names != nil {
		hashes = hash.Set(hash.None)
		for _, name := range names {
			var t hash.Type
			if t.Set(name) == nil && (t == hash.MD5 || t == hash.BLAKE3) {
				hashes.Add(t)
			}
		}
	}
	f.server.hashesMu.Lock()
	defer f.server.hashesMu.Unlock()
	if f.server.hashes != hashes {
		fs.Debugf(f, "Server has hashes %v", hashes)
	}
	f.server.hashes = hashes
	f.server.hashesKnown = true
}

// Hashes returns the supported hash sets.
//
// The server says which hashes it has when it lists the buckets, so
// the first call lists them if that hasn't been done yet.
func (f *Fs) Hashes() hash.Set {
	if len(f.opt.Hashes) > 0 {
		return f.hashes
	}
	f.hashesOnce.Do(func() {
		f.server.hashesMu.Lock()
		known := f.server.hashesKnown
		f.server.hashesMu.Unlock()
		if known {
			return
		}
		_, err := f.listBuckets(f.ctx)
		if err != nil {
			fs.Debugf(f, "Failed to find the hashes of the server, assuming MD5: %v", err)
		}
	})
	f.server.hashesMu.Lock()
	defer f.server.hashesMu.Unlock()
	if !f.server.hashesKnown {
		return hash.Set(hash.MD5)
	}
	return f.server.hashes
}

// FingerprintHashes returns the hashes which may be used in a
// fingerprint.
//
// The BLAKE3 of an object can appear after it was uploaded, for
// example when the server computes it in the background, which would
// make the fingerprint change.
func (f *Fs) FingerprintHashes() hash.Set {
	return f.Hashes() &^ hash.Set(hash.BLAKE3)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the MD5 or BLAKE3 of an object returning a lowercase
// hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t == hash.BLAKE3 && o.fs.Hashes().Contains(hash.BLAKE3) {
		return o.blake3, nil
	}
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// setMetaData sets the metadata from info
func (o *Object) setMetaData(info *api.File) error {
	if info.Kind != "" && info.Kind != api.KindFile {
		return fmt.Errorf("%q is %q not a file: %w", o.remote, info.Kind, fs.ErrorNotAFile)
	}
	modTime, hasFraction, err := info.ModTime()
	if err != nil {
		fs.Debugf(o, "Failed to parse modifiedTime %q: %v", info.ModifiedTime, err)
		modTime = time.Unix(info.UpdatedAt, 0)
	}
	if hasFraction && !o.fs.server.mtimeNanos.Swap(true) {
		fs.Debugf(o.fs, "Server stores sub-second modification times")
	}
	o.id = info.ID
	o.size = info.Size
	o.modTime = modTime
	o.md5 = info.MD5()
	o.blake3 = info.Blake3()
	o.mimeType = info.MimeType
	return nil
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	if o.fs.server.noSetModTime.Load() {
		return fs.ErrorCantSetModTime
	}
	bucket, key := o.split()
	opts := rest.Opts{
		Method: "PATCH",
		Path:   "/content/" + escape(serverPath(bucket, key)),
	}
	req := api.SetModTime{ModifiedTime: modTime.UTC().Format(time.RFC3339Nano)}
	var info api.File
	_, err := o.fs.callJSON(ctx, &opts, &req, &info)
	switch statusCode(err) {
	case http.StatusMethodNotAllowed, http.StatusNotImplemented:
		if !o.fs.server.noSetModTime.Swap(true) {
			fs.Debugf(o.fs, "Server can't set modification times: %v", err)
		}
		return fs.ErrorCantSetModTime
	case http.StatusNotFound:
		return fs.ErrorCantSetModTime
	}
	if err != nil {
		return err
	}
	return o.setMetaData(&info)
}

// Storable returns a boolean showing whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	bucket, key := o.split()
	fs.FixRangeOption(options, o.size)
	opts := rest.Opts{
		Method:  "GET",
		Path:    "/content/" + escape(serverPath(bucket, key)),
		Options: options,
	}
	resp, err := o.fs.call(ctx, &opts, false, func() (*http.Response, error) {
		return o.fs.srv.Call(ctx, &opts)
	})
	if err != nil {
		if statusCode(err) == http.StatusNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	return resp.Body, nil
}

// Update the object with the contents of the io.Reader, modTime and size
//
// If existing is set then it updates the object rather than creating a new one.
//
// The new object may have been created if an error is returned.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	bucket, key := o.split()
	if bucket == "" || key == "" {
		return fs.ErrorCantUploadEmptyFiles
	}
	err := o.fs.makeBucket(ctx, bucket)
	if err != nil {
		return err
	}
	in, uploadHash := o.fs.hashUpload(ctx, in)
	params := url.Values{"mtime": {o.fs.formatMtime(src.ModTime(ctx))}}
	if md5, err := src.Hash(ctx, hash.MD5); err == nil && md5 != "" {
		params.Set("md5", md5)
	}
	opts := rest.Opts{
		Method:      "PUT",
		Path:        "/content/" + escape(serverPath(bucket, key)),
		Body:        in,
		ContentType: fs.MimeType(ctx, src),
		Parameters:  params,
		Options:     options,
	}
	if size := src.Size(); size >= 0 {
		opts.ContentLength = &size
	}
	var info api.File
	// The body can't be rewound so the upload can't be retried here
	_, err = o.fs.call(ctx, &opts, true, func() (*http.Response, error) {
		return o.fs.srv.CallJSON(ctx, &opts, nil, &info)
	})
	if err != nil {
		return fmt.Errorf("failed to upload: %w", err)
	}
	err = o.setMetaData(&info)
	if err != nil {
		return err
	}
	return o.checkUpload(uploadHash)
}

// uploadHash hashes the data of an upload with BLAKE3
type uploadHash struct {
	ctx context.Context
	in  io.Reader
	sem *semaphore.Weighted
	mh  *hash.MultiHasher
}

// Read reads from the upload, hashing what was read
func (h *uploadHash) Read(p []byte) (n int, err error) {
	n, err = h.in.Read(p)
	if n > 0 {
		if semErr := h.sem.Acquire(h.ctx, 1); semErr != nil {
			return n, semErr
		}
		_, _ = h.mh.Write(p[:n])
		h.sem.Release(1)
	}
	return n, err
}

// hashUpload returns in wrapped to hash what is uploaded from it, or
// in and nil if the server has no BLAKE3 to check it against.
func (f *Fs) hashUpload(ctx context.Context, in io.Reader) (io.Reader, *uploadHash) {
	if !f.Hashes().Contains(hash.BLAKE3) {
		return in, nil
	}
	mh, err := hash.NewMultiHasherTypes(hash.NewHashSet(hash.BLAKE3))
	if err != nil {
		fs.Debugf(f, "Not checking uploads: %v", err)
		return in, nil
	}
	// Hash inside the accounting so it still sees the reads
	in, wrap := accounting.UnWrap(in)
	h := &uploadHash{ctx: ctx, in: in, sem: f.hashSem, mh: mh}
	return wrap(h), h
}

// checkUpload returns an error if the BLAKE3 of the data uploaded
// differs from the one the server returned.
func (o *Object) checkUpload(h *uploadHash) error {
	// The server only returns it if it hashed the data inline
	if h == nil || o.blake3 == "" {
		return nil
	}
	sent := h.mh.Sums()[hash.BLAKE3]
	if sent != o.blake3 {
		return fmt.Errorf("upload corrupted: BLAKE3 differ: sent %s but server has %s", sent, o.blake3)
	}
	fs.Debugf(o, "BLAKE3 of upload: %s OK", sent)
	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) error {
	bucket, key := o.split()
	n, err := o.fs.delete(ctx, serverPath(bucket, key))
	if err != nil {
		return err
	}
	if n == 0 {
		return fs.ErrorObjectNotFound
	}
	return nil
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// Check the interfaces are satisfied
var (
	_ fs.Fs                = (*Fs)(nil)
	_ fs.Purger            = (*Fs)(nil)
	_ fs.PutStreamer       = (*Fs)(nil)
	_ fs.Copier            = (*Fs)(nil)
	_ fs.Mover             = (*Fs)(nil)
	_ fs.DirMover          = (*Fs)(nil)
	_ fs.ListRer           = (*Fs)(nil)
	_ fs.FingerprintHasher = (*Fs)(nil)
	_ fs.Object            = (*Object)(nil)
	_ fs.MimeTyper         = (*Object)(nil)
	_ fs.IDer              = (*Object)(nil)
)
