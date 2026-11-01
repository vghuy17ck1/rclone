// KamPlexFS provider support
//
// Everything here is only active when provider = KamPlexFS. The
// server advertises optional features through a capabilities
// endpoint and each one is only used when advertised, so the
// provider falls back to plain S3 behaviour against older servers.

package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	gohash "hash"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	"golang.org/x/sync/semaphore"
)

const kamplexfsProvider = "KamPlexFS"

// Features the KamPlexFS server may advertise in its capabilities
const (
	kamplexfsFeatureRenameObject        = "rename-object"
	kamplexfsFeatureRenamePrefix        = "rename-prefix"
	kamplexfsFeatureSequentialMultipart = "sequential-multipart"
	kamplexfsFeatureMtimeNanos          = "mtime-nanos"
	kamplexfsFeatureChecksums           = "checksums"
	kamplexfsFeatureBlake3              = "blake3"
	kamplexfsFeatureMD5                 = "md5"
	kamplexfsFeatureResumable           = "resumable-upload"
)

// kamplexfsBlake3Header carries the BLAKE3 of an object in HEAD, GET
// and upload responses when the server has computed it.
const kamplexfsBlake3Header = "X-Kamplexfs-Blake3"

// kamplexfsOptions are the provider specific options
var kamplexfsOptions = []fs.Option{{
	Name: "kamplexfs_sequential_upload",
	Help: `Upload the parts of multipart uploads one at a time, in order.

KamPlexFS finishes a multipart upload without re-reading the object
when its parts arrive in order, so this defaults to true and sets the
upload concurrency to 1 unless --s3-upload-concurrency is set
explicitly.

Parts are still sent in parallel if --multi-thread-streams is set
explicitly to a value greater than 1.`,
	Default:  true,
	Advanced: true,
	Provider: kamplexfsProvider,
}, {
	Name: "kamplexfs_exact_modtime",
	Help: `Read modification times with nanosecond precision.

By default the modification time comes from the object listing which
has a precision of 1 second, so no HEAD request is needed per object.

When true and the server supports sub-second modification times, the
modification time is read from the object metadata instead which
costs a HEAD request per object.`,
	Default:  false,
	Advanced: true,
	Provider: kamplexfsProvider,
}, {
	Name: "kamplexfs_blake3_concurrency",
	Help: `Maximum number of BLAKE3 hashes computed at once.

When the server computes BLAKE3 digests, rclone hashes the data of
each upload as it sends it and fails the upload if the digest the
server returns is different. This limits how many uploads are hashed
at the same moment, and so the CPU used for it.

0 means the number of CPUs. Set --s3-disable-checksum to skip the
check.`,
	Default:  0,
	Advanced: true,
	Provider: kamplexfsProvider,
}, {
	Name: "hashes",
	Help: `Hashes to use instead of the ones the server lists.

A comma separated list of md5 and blake3, or none for no hashes.
Leave blank to use the hashes the server lists in its capabilities.

For example md5 turns BLAKE3 off, saving the hashing of uploads, and
blake3 makes rclone compare files by BLAKE3 only. Only list hashes
the server computes, as files have no hash otherwise.`,
	Default:  fs.CommaSepList{},
	Advanced: true,
	Provider: kamplexfsProvider,
}, {
	Name: "kamplexfs_resumable",
	Help: `Use KamPlexFS resumable uploads.

One signed request opens an upload session and the bytes go over TUS,
so a dropped connection resumes from what the server already has.

They are only used if the server supports them. Uploads which set
metadata other than the modification time, content type and tags
are made as multipart uploads, as the session can't store it.`,
	Default: kamplexfsResumableAuto,
	Examples: []fs.OptionExample{{
		Value: kamplexfsResumableAuto.String(),
		Help:  "For uploads of unknown size, and above upload_cutoff when upload_concurrency is 1.",
	}, {
		Value: kamplexfsResumableAlways.String(),
		Help:  "For every upload above upload_cutoff and every unknown size.",
	}, {
		Value: kamplexfsResumableNever.String(),
		Help:  "Do not use them.",
	}},
	Advanced: true,
	Provider: kamplexfsProvider,
}}

// kamplexfsResumableMode says when to use resumable uploads
type kamplexfsResumableMode = fs.Enum[kamplexfsResumableChoices]

// Values of kamplexfsResumableMode
const (
	kamplexfsResumableAuto kamplexfsResumableMode = iota
	kamplexfsResumableAlways
	kamplexfsResumableNever
)

type kamplexfsResumableChoices struct{}

func (kamplexfsResumableChoices) Choices() []string {
	return []string{
		kamplexfsResumableAuto:   "auto",
		kamplexfsResumableAlways: "always",
		kamplexfsResumableNever:  "never",
	}
}

// kamplexfsOpt holds the provider specific options
type kamplexfsOpt struct {
	SequentialUpload  bool                   `config:"kamplexfs_sequential_upload"`
	ExactModTime      bool                   `config:"kamplexfs_exact_modtime"`
	Blake3Concurrency int                    `config:"kamplexfs_blake3_concurrency"`
	Hashes            fs.CommaSepList        `config:"hashes"`
	Resumable         kamplexfsResumableMode `config:"kamplexfs_resumable"`
}

// kamplexfsCapabilities is the response from the capabilities endpoint
type kamplexfsCapabilities struct {
	Version     string   `json:"version"`
	StorageMode string   `json:"storageMode"`
	MaxKeys     int      `json:"maxKeys"`
	Features    []string `json:"features"`
}

// kamplexfs holds the KamPlexFS state of an Fs
type kamplexfs struct {
	opt          kamplexfsOpt
	caps         kamplexfsCapabilities
	features     map[string]bool
	exactModTime bool // read the mtime from the metadata with 1ns precision
	hashes       hash.Set
	blake3       *kamplexfsBlake3Cache // nil unless the server has BLAKE3
	hashSem      *semaphore.Weighted   // limits the BLAKE3 hashing of uploads
	headUploads  bool                  // HEAD multipart uploads for their BLAKE3
	resumable    atomic.Bool           // use resumable uploads
}

// has returns true if the server advertised the feature
func (k *kamplexfs) has(feature string) bool {
	return k.features[feature]
}

// hashSet returns the hashes to use, from the hashes option
// if set or else from the capabilities.
func (k *kamplexfs) hashSet() (hash.Set, error) {
	if len(k.opt.Hashes) > 0 {
		hashes := hash.Set(hash.None)
		for _, name := range k.opt.Hashes {
			var t hash.Type
			err := t.Set(strings.TrimSpace(name))
			if err != nil || (t != hash.None && t != hash.MD5 && t != hash.BLAKE3) {
				return hashes, fmt.Errorf("hashes: %q isn't md5, blake3 or none", name)
			}
			hashes.Add(t)
		}
		return hashes, nil
	}
	if !k.has(kamplexfsFeatureBlake3) {
		return hash.Set(hash.MD5), nil
	}
	// Servers which list blake3 list md5 too when they serve MD5s,
	// so MD5 is only off if blake3 is listed alone.
	if !k.has(kamplexfsFeatureMD5) {
		return hash.Set(hash.BLAKE3), nil
	}
	return hash.NewHashSet(hash.MD5, hash.BLAKE3), nil
}

// kamplexfsRenamePrefixResult is the response to a prefix rename
type kamplexfsRenamePrefixResult struct {
	ObjectsRenamed int64 `xml:"ObjectsRenamed"`
}

// kamplexfsError is an S3 style error returned from a request made
// outside the SDK.
//
// It satisfies smithy.APIError and has an HTTPStatusCode method so it
// is treated like an SDK error by shouldRetry and getHTTPStatusCode.
type kamplexfsError struct {
	StatusCode int    `xml:"-"`
	Code       string `xml:"Code"`
	Message    string `xml:"Message"`
}

// Error satisfies the error interface
func (e *kamplexfsError) Error() string {
	return fmt.Sprintf("KamPlexFS: HTTP %d: %s: %s", e.StatusCode, e.Code, e.Message)
}

// ErrorCode returns the S3 error code
func (e *kamplexfsError) ErrorCode() string { return e.Code }

// ErrorMessage returns the S3 error message
func (e *kamplexfsError) ErrorMessage() string { return e.Message }

// ErrorFault returns the fault
func (e *kamplexfsError) ErrorFault() smithy.ErrorFault {
	if e.StatusCode >= 500 {
		return smithy.FaultServer
	}
	return smithy.FaultClient
}

// HTTPStatusCode returns the HTTP status code
func (e *kamplexfsError) HTTPStatusCode() int { return e.StatusCode }

var _ smithy.APIError = (*kamplexfsError)(nil)

// optionIsSet returns true if the user set the option explicitly
// rather than it taking its default value.
func optionIsSet(m configmap.Mapper, key string) bool {
	if cm, ok := m.(*configmap.Map); ok {
		_, ok = cm.GetPriority(key, configmap.PriorityConfig)
		return ok
	}
	_, ok := m.Get(key)
	return ok
}

// kamplexfsSetup configures the Fs for the KamPlexFS provider
//
// It does nothing for other providers.
func (f *Fs) kamplexfsSetup(ctx context.Context, m configmap.Mapper) error {
	if f.opt.Provider != kamplexfsProvider {
		return nil
	}
	k := &kamplexfs{features: map[string]bool{}}
	err := configstruct.Set(m, &k.opt)
	if err != nil {
		return err
	}
	f.kpx = k

	caps, err := f.kamplexfsFetchCapabilities(ctx)
	if err != nil {
		fs.Debugf(f, "KamPlexFS: capabilities not available, using plain S3: %v", err)
	} else {
		k.caps = *caps
		for _, feature := range caps.Features {
			k.features[feature] = true
		}
		fs.Debugf(f, "KamPlexFS: server version %q, storage mode %q, maxKeys %d, features %v", caps.Version, caps.StorageMode, caps.MaxKeys, caps.Features)
	}

	// The server verifies Content-MD5 and returns the MD5 as the
	// ETag, so the HEAD after each upload adds nothing, except to
	// read the BLAKE3 of multipart uploads.
	if !optionIsSet(m, "no_head") {
		f.opt.NoHead = true
		k.headUploads = true
	}
	if !optionIsSet(m, "list_chunk") && k.caps.MaxKeys > 0 {
		f.opt.ListChunk = int32(min(k.caps.MaxKeys, math.MaxInt32))
	}
	if k.opt.SequentialUpload && !optionIsSet(m, "upload_concurrency") {
		f.opt.UploadConcurrency = 1
	}
	if k.has(kamplexfsFeatureChecksums) && !optionIsSet(m, "use_data_integrity_protections") {
		// The SDK only sends checksums of streamed bodies as
		// aws-chunked trailers, and only over TLS. Over plain HTTP
		// it refuses to upload unseekable bodies instead.
		if strings.HasPrefix(strings.ToLower(f.opt.Endpoint), "https:") {
			f.opt.UseDataIntegrityProtections = fs.Tristate{Valid: true, Value: true}
			f.c = s3.New(f.c.Options(), func(o *s3.Options) {
				o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenSupported
				o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenSupported
			})
		} else {
			fs.Debugf(f, "KamPlexFS: not enabling data integrity protections as the endpoint is not https")
		}
	}

	k.exactModTime = k.opt.ExactModTime && k.has(kamplexfsFeatureMtimeNanos)
	if !k.exactModTime {
		f.features.SlowModTime = false
	}
	k.hashes, err = k.hashSet()
	if err != nil {
		return err
	}
	if k.hashes.Contains(hash.BLAKE3) {
		k.blake3 = newKamplexfsBlake3Cache()
		n := k.opt.Blake3Concurrency
		if n <= 0 {
			n = runtime.NumCPU()
		}
		k.hashSem = semaphore.NewWeighted(int64(n))
	}
	if k.opt.Resumable != kamplexfsResumableNever {
		if k.has(kamplexfsFeatureResumable) {
			k.resumable.Store(true)
			// Multi-thread copies upload through the chunk writer,
			// so only keep it for uploads made in parallel.
			if !fs.GetConfig(ctx).MultiThreadSet && f.kamplexfsResumable(0) {
				f.features.OpenChunkWriter = nil
			}
		} else {
			fs.Debugf(f, "KamPlexFS: server has no resumable uploads, using multipart uploads")
		}
	}
	if k.has(kamplexfsFeatureRenameObject) {
		f.features.Move = f.kamplexfsMove
	}
	if k.has(kamplexfsFeatureRenamePrefix) {
		f.features.DirMove = f.kamplexfsDirMove
	}
	return nil
}

// kamplexfsURL makes a URL for a request to the server
//
// The URL is built from the endpoint so any path prefix it has is
// kept. If bucket is empty the URL points at the endpoint root.
func (f *Fs) kamplexfsURL(bucket, key, query string) (*url.URL, error) {
	if f.opt.Endpoint == "" {
		return nil, errors.New("no endpoint configured")
	}
	u := strings.TrimSuffix(f.opt.Endpoint, "/") + "/"
	if bucket != "" {
		u += pathEscape(bucket) + "/" + pathEscape(key)
	}
	return url.Parse(u + "?" + query)
}

// kamplexfsCall makes a SigV4 signed request with an empty body to
// the server, returning the response body on success or a
// *kamplexfsError.
func (f *Fs) kamplexfsCall(ctx context.Context, method string, u *url.URL, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	c := f.c.Options()
	var creds aws.Credentials
	err = errors.New("anonymous access")
	if c.Credentials != nil {
		creds, err = c.Credentials.Retrieve(ctx)
	}
	if err != nil {
		fs.Debugf(f, "KamPlexFS: sending unsigned request as no credentials: %v", err)
	} else {
		const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)
		signer := v4signer.NewSigner(func(o *v4signer.SignerOptions) {
			// S3 signs the path as sent rather than escaping it again
			o.DisableURIPathEscaping = true
		})
		err = signer.SignHTTP(ctx, creds, req, emptyPayloadHash, "s3", c.Region, time.Now().UTC())
		if err != nil {
			return nil, fmt.Errorf("failed to sign request: %w", err)
		}
	}
	resp, err := f.srv.Do(req)
	if err != nil {
		return nil, err
	}
	defer fs.CheckClose(resp.Body, &err)
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		apiErr := &kamplexfsError{}
		if xml.Unmarshal(body, apiErr) != nil || apiErr.Code == "" {
			apiErr.Code = http.StatusText(resp.StatusCode)
		}
		apiErr.StatusCode = resp.StatusCode
		return nil, apiErr
	}
	return body, nil
}

// kamplexfsFetchCapabilities reads the capabilities of the server
func (f *Fs) kamplexfsFetchCapabilities(ctx context.Context) (*kamplexfsCapabilities, error) {
	u, err := f.kamplexfsURL("", "", "kamplexfsCapabilities")
	if err != nil {
		return nil, err
	}
	body, err := f.kamplexfsCall(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	var caps kamplexfsCapabilities
	err = json.Unmarshal(body, &caps)
	if err != nil {
		return nil, fmt.Errorf("failed to decode capabilities: %w", err)
	}
	return &caps, nil
}

// kamplexfsPrecision returns the modtime precision or 0 if the
// provider isn't KamPlexFS.
func (f *Fs) kamplexfsPrecision() time.Duration {
	if f.kpx == nil || f.kpx.exactModTime {
		return 0
	}
	// Listings only carry whole seconds
	return time.Second
}

// kamplexfsTruncateMtime returns the modification time to write
//
// Servers without sub-second mtimes replace an mtime with a fraction
// by the upload time, so send them whole seconds only.
func (f *Fs) kamplexfsTruncateMtime(t time.Time) time.Time {
	if f.kpx == nil || f.kpx.has(kamplexfsFeatureMtimeNanos) {
		return t
	}
	return t.Truncate(time.Second)
}

// kamplexfsModTime returns the modification time without a HEAD
// request and true, or false if the stock method should be used.
//
// The server reports the client's mtime floored to whole seconds as
// the LastModified in listings and HEAD, so that is used unless the
// mtime metadata is already known, for example just after an upload.
func (o *Object) kamplexfsModTime() (time.Time, bool) {
	if o.fs.kpx == nil || o.fs.kpx.exactModTime {
		return time.Time{}, false
	}
	if d, ok := o.meta[metaMtime]; ok {
		modTime, err := swift.FloatStringToTime(d)
		if err == nil {
			// The metadata may have a fraction which the listings
			// don't, and fs.Fingerprint uses the full mtime so the
			// VFS cache would drop the file as changed.
			return modTime.Truncate(time.Second), true
		}
		fs.Debugf(o, "Failed to read mtime from metadata: %v", err)
	}
	return o.lastModified, true
}

// kamplexfsSameServer returns true if g talks to the same KamPlexFS
// server with the same credentials as f.
func (f *Fs) kamplexfsSameServer(g *Fs) bool {
	return g.kpx != nil &&
		g.opt.Endpoint == f.opt.Endpoint &&
		g.opt.AccessKeyID == f.opt.AccessKeyID
}

// kamplexfsMove moves src to this remote with a single RenameObject
//
// It returns fs.ErrorCantMove if it isn't possible so rclone falls
// back to copy and delete.
func (f *Fs) kamplexfsMove(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if f.opt.VersionAt.IsSet() {
		return nil, errNotWithVersionAt
	}
	srcObj, ok := src.(*Object)
	if !ok || !f.kamplexfsSameServer(srcObj.fs) {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	err := f.mkdirParent(ctx, remote)
	if err != nil {
		return nil, err
	}
	dstBucket, dstPath := f.split(remote)
	srcBucket, srcPath := srcObj.split()
	req := s3.RenameObjectInput{
		Bucket:       &dstBucket,
		Key:          &dstPath,
		RenameSource: aws.String("/" + pathEscape(srcBucket+"/"+srcPath)),
	}
	err = f.pacer.Call(func() (bool, error) {
		_, err := f.c.RenameObject(ctx, &req)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		switch getHTTPStatusCode(err) {
		case http.StatusNotFound:
			return nil, fs.ErrorObjectNotFound
		case http.StatusBadRequest, http.StatusNotImplemented:
			fs.Debugf(src, "Can't move with RenameObject: %v", err)
			return nil, fs.ErrorCantMove
		}
		return nil, err
	}
	// A rename leaves the size, ETag, mtime and metadata unchanged
	// so there is no need to HEAD the new object.
	dstObj := *srcObj
	dstObj.fs = f
	dstObj.remote = remote
	dstObj.versionID = nil
	dstObj.meta = maps.Clone(srcObj.meta)
	return &dstObj, nil
}

// kamplexfsDirMove moves src, srcRemote to this remote at dstRemote
// with a single prefix rename.
//
// It returns fs.ErrorCantDirMove if it isn't possible so rclone
// falls back to moving the objects one by one, or fs.ErrorDirExists
// if the destination already has objects.
func (f *Fs) kamplexfsDirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	if f.opt.VersionAt.IsSet() {
		return errNotWithVersionAt
	}
	srcFs, ok := src.(*Fs)
	if !ok || !f.kamplexfsSameServer(srcFs) {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	srcBucket, srcPath := srcFs.split(srcRemote)
	dstBucket, dstPath := f.split(dstRemote)
	if srcBucket == "" || dstBucket == "" {
		fs.Debugf(srcFs, "Can't move directory - can't move buckets")
		return fs.ErrorCantDirMove
	}
	err := f.makeBucket(ctx, dstBucket)
	if err != nil {
		return err
	}
	srcPrefix := kamplexfsDirPrefix(srcPath)
	dstPrefix := kamplexfsDirPrefix(dstPath)
	u, err := f.kamplexfsURL(dstBucket, dstPrefix, "kamplexfsRenamePrefix")
	if err != nil {
		return err
	}
	headers := map[string]string{
		"X-Kamplexfs-Rename-Source": "/" + pathEscape(srcBucket+"/"+srcPrefix),
	}
	var body []byte
	err = f.pacer.Call(func() (bool, error) {
		body, err = f.kamplexfsCall(ctx, http.MethodPut, u, headers)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		var apiErr smithy.APIError
		code := ""
		if errors.As(err, &apiErr) {
			code = apiErr.ErrorCode()
		}
		switch status := getHTTPStatusCode(err); {
		case status == http.StatusConflict && code == "DestinationNotEmpty":
			return fs.ErrorDirExists
		case status == http.StatusNotFound:
			return fs.ErrorDirNotFound
		case status == http.StatusBadRequest || status == http.StatusNotImplemented:
			fs.Debugf(srcFs, "Can't move directory with prefix rename: %v", err)
			return fs.ErrorCantDirMove
		}
		return err
	}
	var result kamplexfsRenamePrefixResult
	if xml.Unmarshal(body, &result) == nil {
		fs.Debugf(f, "KamPlexFS: renamed %d objects from %q to %q", result.ObjectsRenamed, srcFs.root+"/"+srcRemote, f.root+"/"+dstRemote)
	}
	if srcPrefix == "" {
		// Moving a whole bucket leaves it behind empty
		return srcFs.Rmdir(ctx, srcRemote)
	}
	return nil
}

// kamplexfsDirPrefix returns dir as a prefix ending in "/", or "" for
// the root of the bucket.
func kamplexfsDirPrefix(dir string) string {
	if dir == "" || strings.HasSuffix(dir, "/") {
		return dir
	}
	return dir + "/"
}

// kamplexfsHashes returns the hashes of the server and true, or
// false if the provider isn't KamPlexFS.
func (f *Fs) kamplexfsHashes() (hash.Set, bool) {
	if f.kpx == nil {
		return 0, false
	}
	return f.kpx.hashes, true
}

// FingerprintHashes returns the hashes which may be used in a
// fingerprint.
//
// Listings don't carry the BLAKE3, so a fingerprint with it would
// differ between an object just uploaded and the same object listed.
func (f *Fs) FingerprintHashes() hash.Set {
	hashes := f.Hashes()
	return hashes &^ hash.Set(hash.BLAKE3)
}

var _ fs.FingerprintHasher = (*Fs)(nil)

// matchBlake3 matches a BLAKE3 digest as the server sends it
var matchBlake3 = regexp.MustCompile(`^[0-9a-f]{64}$`)

// kamplexfsSetBlake3 sets the BLAKE3 from the response the metadata
// came with, if there was one.
//
// Metadata which wasn't read from a response, for example made up
// after an upload, leaves the BLAKE3 alone.
func (o *Object) kamplexfsSetBlake3(metadata middleware.Metadata) {
	if o.fs.kpx == nil {
		return
	}
	if resp, ok := awsmiddleware.GetRawResponse(metadata).(*smithyhttp.Response); ok && resp.Response != nil {
		o.kamplexfsSetBlake3FromHeader(resp.Header)
	}
}

// kamplexfsSetBlake3FromHeader sets the BLAKE3 from a response header
//
// A missing header means the server hasn't computed it yet.
func (o *Object) kamplexfsSetBlake3FromHeader(header http.Header) {
	if o.fs.kpx == nil || o.fs.kpx.blake3 == nil {
		return
	}
	digest := header.Get(kamplexfsBlake3Header)
	if digest != "" && !matchBlake3.MatchString(digest) {
		fs.Debugf(o, "KamPlexFS: ignoring invalid BLAKE3 %q", digest)
		digest = ""
	}
	o.blake3 = digest
}

// kamplexfsBlake3 returns the BLAKE3 of the object, or "" if the
// server hasn't computed it.
//
// Objects from a listing get it from a bulk listing of their
// directory if more than one of them is asked for, and from a HEAD
// request otherwise.
func (o *Object) kamplexfsBlake3(ctx context.Context) (string, error) {
	if o.fs.kpx.blake3 == nil {
		return "", hash.ErrUnsupported
	}
	// If decompressing, erase the hash
	if o.bytes < 0 {
		return "", nil
	}
	// Uploads, HEAD and GET responses have set it already
	if o.blake3 != "" || o.meta != nil {
		return o.blake3, nil
	}
	bucket, key := o.split()
	digest, found := o.fs.kpx.blake3.lookup(ctx, o, bucket, key)
	if found {
		o.blake3 = digest
		return digest, nil
	}
	err := o.readMetaData(ctx)
	if err != nil {
		return "", err
	}
	return o.blake3, nil
}

// kamplexfsHeadUpload returns true if the upload hashed by h must be
// HEADed to read its BLAKE3 even with no_head.
//
// noDigest is true for multipart uploads, as CompleteMultipartUpload
// responses can't carry the BLAKE3 header: the server keeps the
// connection alive by sending the body before it has finished, and can
// only send a fixed set of headers after it. It is also true for
// resumable uploads whose last response had no BLAKE3.
func (f *Fs) kamplexfsHeadUpload(h *kamplexfsUploadHash, noDigest bool) bool {
	return h != nil && noDigest && f.kpx.headUploads
}

// kamplexfsUploadHash hashes the data of an upload with BLAKE3
type kamplexfsUploadHash struct {
	ctx context.Context
	in  io.Reader
	sem *semaphore.Weighted
	mh  *hash.MultiHasher
}

// Read reads from the upload, hashing what was read
func (h *kamplexfsUploadHash) Read(p []byte) (n int, err error) {
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

// kamplexfsHashUpload returns in wrapped to hash what is uploaded
// from it, or in and nil if the upload isn't checked.
func (f *Fs) kamplexfsHashUpload(ctx context.Context, in io.Reader) (io.Reader, *kamplexfsUploadHash) {
	if f.kpx == nil || f.kpx.blake3 == nil || f.opt.DisableChecksum {
		return in, nil
	}
	mh, err := hash.NewMultiHasherTypes(hash.NewHashSet(hash.BLAKE3))
	if err != nil {
		fs.Debugf(f, "KamPlexFS: not checking uploads: %v", err)
		return in, nil
	}
	// Hash inside the accounting so the upload code still finds it
	in, wrap := accounting.UnWrap(in)
	h := &kamplexfsUploadHash{ctx: ctx, in: in, sem: f.kpx.hashSem, mh: mh}
	return wrap(h), h
}

// kamplexfsCheckUpload returns an error if the BLAKE3 of the data
// uploaded differs from the one the server returned.
func (o *Object) kamplexfsCheckUpload(h *kamplexfsUploadHash) error {
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

// Limits of the BLAKE3 cache
const (
	// kamplexfsBlake3TTL is how long digests from a bulk listing are
	// used for
	kamplexfsBlake3TTL = time.Minute
	// kamplexfsBlake3Max is the most digests cached at once
	kamplexfsBlake3Max = 100000
	// kamplexfsBlake3PageSize is the most digests asked for at once
	kamplexfsBlake3PageSize = 1000
)

// kamplexfsBlake3Cache holds the BLAKE3 digests from bulk listings
// until they are asked for.
//
// A directory is bulk listed the second time one of its objects is
// asked for within kamplexfsBlake3TTL, so a single object costs a
// HEAD request and hashing a whole directory costs a request per
// kamplexfsBlake3PageSize objects.
type kamplexfsBlake3Cache struct {
	mu      sync.Mutex
	fetches map[string]*kamplexfsBlake3Fetch // bulk listings by bucket and directory
	digests map[string]kamplexfsBlake3Digest // digests by bucket and key
	misses  map[string]time.Time             // last miss by bucket and directory
	now     func() time.Time                 // for testing
}

// kamplexfsBlake3Fetch is a bulk listing of a directory
type kamplexfsBlake3Fetch struct {
	done    chan struct{} // closed when the listing has finished
	expires time.Time     // when the digests stop being used
	err     error         // error the listing stopped with
}

// kamplexfsBlake3Digest is a digest from a bulk listing
type kamplexfsBlake3Digest struct {
	fetch        *kamplexfsBlake3Fetch
	size         int64
	etag         string    // "" if not known
	lastModified time.Time // zero if not known
	blake3       string    // "" if not computed
}

// kamplexfsHashList is the response to a bulk listing
type kamplexfsHashList struct {
	Objects []struct {
		Key          string  `json:"key"`
		Size         int64   `json:"size"`
		ETag         string  `json:"etag"`
		LastModified string  `json:"lastModified"`
		Blake3       *string `json:"blake3"`
	} `json:"objects"`
	NextContinuationToken *string `json:"nextContinuationToken"`
}

// newKamplexfsBlake3Cache makes an empty kamplexfsBlake3Cache
func newKamplexfsBlake3Cache() *kamplexfsBlake3Cache {
	return &kamplexfsBlake3Cache{
		fetches: map[string]*kamplexfsBlake3Fetch{},
		digests: map[string]kamplexfsBlake3Digest{},
		misses:  map[string]time.Time{},
		now:     time.Now,
	}
}

// expired returns true if the listing has finished and its digests
// are too old to use.
func (fetch *kamplexfsBlake3Fetch) expired(now time.Time) bool {
	return !fetch.expires.IsZero() && now.After(fetch.expires)
}

// lookup returns the BLAKE3 of the object o, which is bucket/key, and
// true if a bulk listing had it, or false if it should be read with
// a HEAD request.
func (c *kamplexfsBlake3Cache) lookup(ctx context.Context, o *Object, bucket, key string) (digest string, found bool) {
	c.mu.Lock()
	now := c.now()
	dir := key[:strings.LastIndex(key, "/")+1]
	dirKey := bucket + "/" + dir
	fetch := c.fetches[dirKey]
	if fetch != nil && fetch.expired(now) {
		fetch = nil
	}
	if fetch == nil {
		if last, ok := c.misses[dirKey]; !ok || now.Sub(last) > kamplexfsBlake3TTL {
			c.misses[dirKey] = now
			if len(c.misses) > kamplexfsBlake3PageSize {
				c.sweep(now)
			}
			c.mu.Unlock()
			return "", false
		}
		delete(c.misses, dirKey)
		c.sweep(now)
		fetch = &kamplexfsBlake3Fetch{done: make(chan struct{})}
		c.fetches[dirKey] = fetch
		c.mu.Unlock()
		c.fill(ctx, o.fs, bucket, dir, fetch)
	} else {
		c.mu.Unlock()
		select {
		case <-fetch.done:
		case <-ctx.Done():
			return "", false
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.digests[bucket+"/"+key]
	if !ok || !d.matches(o) {
		// The object may have changed since it was listed, or the
		// listing stopped early or failed
		return "", false
	}
	// Each object is usually only asked for once
	delete(c.digests, bucket+"/"+key)
	return d.blake3, true
}

// matches returns true if d is for the same data as the object o
func (d *kamplexfsBlake3Digest) matches(o *Object) bool {
	if d.size != o.bytes {
		return false
	}
	if !d.lastModified.IsZero() && !d.lastModified.Equal(o.lastModified.Truncate(time.Second)) {
		return false
	}
	// o.md5 is the ETag of the object if it is an MD5
	return d.etag == "" || o.md5 == "" || d.etag == o.md5
}

// fill bulk lists bucket/prefix into the cache and marks fetch done
func (c *kamplexfsBlake3Cache) fill(ctx context.Context, f *Fs, bucket, prefix string, fetch *kamplexfsBlake3Fetch) {
	var (
		token string
		err   error
	)
	defer func() {
		c.mu.Lock()
		fetch.err = err
		fetch.expires = c.now().Add(kamplexfsBlake3TTL)
		c.mu.Unlock()
		close(fetch.done)
	}()
	for {
		var list *kamplexfsHashList
		list, err = f.kamplexfsListHashes(ctx, bucket, prefix, token)
		if err != nil {
			fs.Debugf(f, "KamPlexFS: failed to list BLAKE3 digests of %q: %v", bucket+"/"+prefix, err)
			return
		}
		c.mu.Lock()
		for _, object := range list.Objects {
			d := kamplexfsBlake3Digest{
				fetch:  fetch,
				size:   object.Size,
				etag:   strings.Trim(strings.ToLower(object.ETag), `"`),
				blake3: deref(object.Blake3),
			}
			if !matchBlake3.MatchString(d.blake3) {
				d.blake3 = ""
			}
			if object.LastModified != "" {
				d.lastModified, err = time.Parse(time.RFC3339, object.LastModified)
				if err != nil {
					// Don't use a digest which can't be checked
					fs.Debugf(f, "KamPlexFS: bad lastModified in BLAKE3 listing: %v", err)
					err = nil
					continue
				}
			}
			c.digests[bucket+"/"+object.Key] = d
		}
		full := len(c.digests) >= kamplexfsBlake3Max
		c.mu.Unlock()
		token = deref(list.NextContinuationToken)
		if token == "" {
			return
		}
		if full {
			// The keys not listed are read with HEAD instead
			fs.Debugf(f, "KamPlexFS: BLAKE3 cache full in %q", bucket+"/"+prefix)
			return
		}
	}
}

// sweep removes the expired listings and digests
//
// Call with c.mu held.
func (c *kamplexfsBlake3Cache) sweep(now time.Time) {
	for k, fetch := range c.fetches {
		if fetch.expired(now) {
			delete(c.fetches, k)
		}
	}
	for k, d := range c.digests {
		if d.fetch.expired(now) {
			delete(c.digests, k)
		}
	}
	for k, last := range c.misses {
		if now.Sub(last) > kamplexfsBlake3TTL {
			delete(c.misses, k)
		}
	}
}

// kamplexfsListHashes reads a page of the bulk listing of the BLAKE3
// digests of the objects in bucket directly under prefix.
func (f *Fs) kamplexfsListHashes(ctx context.Context, bucket, prefix, token string) (*kamplexfsHashList, error) {
	if f.opt.Endpoint == "" {
		return nil, errors.New("no endpoint configured")
	}
	query := url.Values{}
	query.Set("prefix", prefix)
	query.Set("delimiter", "/")
	query.Set("max-keys", strconv.Itoa(min(int(f.opt.ListChunk), kamplexfsBlake3PageSize)))
	if token != "" {
		query.Set("continuation-token", token)
	}
	u, err := url.Parse(strings.TrimSuffix(f.opt.Endpoint, "/") + "/" + pathEscape(bucket) + "?kamplexfsHashes&" + query.Encode())
	if err != nil {
		return nil, err
	}
	var body []byte
	err = f.pacer.Call(func() (bool, error) {
		body, err = f.kamplexfsCall(ctx, http.MethodGet, u, nil)
		return f.shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	var list kamplexfsHashList
	err = json.Unmarshal(body, &list)
	if err != nil {
		return nil, fmt.Errorf("failed to decode BLAKE3 listing: %w", err)
	}
	return &list, nil
}

// kamplexfsTusVersion is the TUS protocol version of resumable uploads
const kamplexfsTusVersion = "1.0.0"

// errKamplexfsNoResumable is returned by kamplexfsUploadResumable
// before it has read any data if the upload must be made another way.
var errKamplexfsNoResumable = errors.New("KamPlexFS: can't use a resumable upload")

// errKamplexfsSessionGone is returned if the resumable upload
// session is completed, deleted or expired.
var errKamplexfsSessionGone = errors.New("KamPlexFS: resumable upload session not found")

// kamplexfsResumable returns true if an upload of size bytes, or of
// unknown size if size < 0, which is too big for a single part uses
// a resumable upload.
func (f *Fs) kamplexfsResumable(size int64) bool {
	if f.kpx == nil || !f.kpx.resumable.Load() {
		return false
	}
	return size < 0 || f.kpx.opt.Resumable == kamplexfsResumableAlways || f.opt.UploadConcurrency == 1
}

// kamplexfsResumableUnsupported returns why req can't be made as a
// resumable upload, or "" if it can.
//
// Opening the session only stores the content type, the modification
// time and the tags.
func kamplexfsResumableUnsupported(req *s3.PutObjectInput) string {
	for key := range req.Metadata {
		if key != metaMtime {
			return fmt.Sprintf("it has %q metadata", key)
		}
	}
	switch {
	case req.CacheControl != nil || req.ContentDisposition != nil || req.ContentEncoding != nil || req.ContentLanguage != nil:
		return "it has content headers"
	case req.IfMatch != nil || req.IfNoneMatch != nil:
		return "it is conditional"
	case req.ACL != "" || req.StorageClass != "":
		return "it has an ACL or storage class"
	case req.ServerSideEncryption != "" || req.SSECustomerAlgorithm != nil || req.SSEKMSKeyId != nil:
		return "it is encrypted"
	case req.ObjectLockMode != "" || req.ObjectLockRetainUntilDate != nil || req.ObjectLockLegalHoldStatus != "":
		return "it has an object lock"
	}
	return ""
}

// kamplexfsRedact returns the session URL without its query, which
// holds the token authorising the session.
func kamplexfsRedact(location string) string {
	before, _, _ := strings.Cut(location, "?")
	return before
}

// kamplexfsUploadResumable uploads in with a resumable upload
//
// size is the size of the upload or -1 if it isn't known. It returns
// the size uploaded and the ETag the server returned.
//
// It returns errKamplexfsNoResumable before reading from in if the
// upload can't be made this way.
func (o *Object) kamplexfsUploadResumable(ctx context.Context, src fs.ObjectInfo, in io.Reader, size int64, options ...fs.OpenOption) (ui uploadInfo, n int64, etag string, err error) {
	ui, err = o.prepareUpload(ctx, src, options, true)
	if err != nil {
		return ui, size, "", fmt.Errorf("failed to prepare upload: %w", err)
	}
	if reason := kamplexfsResumableUnsupported(ui.req); reason != "" {
		fs.Debugf(o, "KamPlexFS: not using a resumable upload as %s", reason)
		return ui, size, "", errKamplexfsNoResumable
	}
	location, err := o.kamplexfsOpenSession(ctx, ui.req, size)
	if err != nil {
		return ui, size, "", err
	}
	bufSize := int64(o.fs.opt.ChunkSize)
	if size >= 0 {
		bufSize = min(bufSize, size)
	}
	t := &kamplexfsTus{
		o:        o,
		location: location,
		size:     size,
		in:       in,
		buf:      make([]byte, 0, bufSize),
	}
	if o.fs.kpx.has(kamplexfsFeatureMD5) && !o.fs.opt.DisableChecksum {
		t.md5 = md5.New()
	}
	err = t.upload(ctx)
	if err != nil {
		if !errors.Is(err, errKamplexfsSessionGone) {
			t.abort(ctx)
		}
		return ui, size, "", err
	}
	if t.md5 != nil && t.etag != "" {
		sent := hex.EncodeToString(t.md5.Sum(nil))
		got := strings.Trim(strings.ToLower(t.etag), `"`)
		if sent != got {
			return ui, size, "", fmt.Errorf("upload corrupted: MD5 differ: sent %s but server has %s", sent, got)
		}
		fs.Debugf(o, "MD5 of upload: %s OK", sent)
	}
	return ui, t.offset, t.etag, nil
}

// kamplexfsSession is the response to opening a resumable upload
type kamplexfsSession struct {
	UploadID string `json:"uploadId"`
	Location string `json:"location"`
}

// kamplexfsOpenSession opens a resumable upload of size bytes, or of
// unknown size if size < 0, returning the session URL.
func (o *Object) kamplexfsOpenSession(ctx context.Context, req *s3.PutObjectInput, size int64) (string, error) {
	query := "kamplexfsResumable"
	if size >= 0 {
		query += "&kamplexfsUploadLength=" + strconv.FormatInt(size, 10)
	}
	u, err := o.fs.kamplexfsURL(*req.Bucket, *req.Key, query)
	if err != nil {
		return "", err
	}
	headers := map[string]string{
		"Content-Type":     deref(req.ContentType),
		"X-Amz-Meta-Mtime": req.Metadata[metaMtime],
	}
	if req.Tagging != nil {
		headers["X-Amz-Tagging"] = *req.Tagging
	}
	var body []byte
	err = o.fs.pacer.Call(func() (bool, error) {
		body, err = o.fs.kamplexfsCall(ctx, http.MethodPost, u, headers)
		return o.fs.shouldRetry(ctx, err)
	})
	if getHTTPStatusCode(err) == http.StatusNotImplemented {
		fs.Debugf(o, "KamPlexFS: server has resumable uploads turned off, using multipart uploads: %v", err)
		o.fs.kpx.resumable.Store(false)
		return "", errKamplexfsNoResumable
	}
	if err != nil {
		return "", fmt.Errorf("failed to open resumable upload: %w", err)
	}
	var session kamplexfsSession
	err = json.Unmarshal(body, &session)
	if err != nil {
		return "", fmt.Errorf("failed to decode resumable upload: %w", err)
	}
	loc, err := url.Parse(session.Location)
	if err != nil || (loc.Scheme != "http" && loc.Scheme != "https") || loc.Host == "" {
		return "", fmt.Errorf("resumable upload has a bad location %q", kamplexfsRedact(session.Location))
	}
	// A server behind a TLS proxy which doesn't say so returns http URLs
	fs.Debugf(o, "KamPlexFS: resumable upload %s at %s", session.UploadID, kamplexfsRedact(session.Location))
	return session.Location, nil
}

// kamplexfsTus sends the data of a resumable upload with TUS
//
// The data is sent a chunk at a time, one PATCH per chunk, and each
// chunk is held until the server has all of it so whatever didn't
// arrive can be sent again.
type kamplexfsTus struct {
	o        *Object
	location string    // the session URL, which authorises the requests
	size     int64     // size of the upload, -1 if not known
	in       io.Reader // the data to upload
	md5      gohash.Hash
	offset   int64  // bytes the server has
	buf      []byte // the chunk held, read from in so far
	start    int64  // offset of buf
	eof      bool   // in is exhausted
	inErr    error  // error reading in
	final    bool   // a PATCH which completes the upload was sent
	etag     string // ETag of the uploaded object
}

// held returns the offset of the end of the data read from in
func (t *kamplexfsTus) held() int64 {
	return t.start + int64(len(t.buf))
}

// chunkEnd returns the offset of the end of the chunk held
func (t *kamplexfsTus) chunkEnd() int64 {
	end := t.start + int64(cap(t.buf))
	if t.size >= 0 {
		end = min(end, t.size)
	}
	return end
}

// length returns the size of the upload, or -1 if it isn't known yet
func (t *kamplexfsTus) length() int64 {
	if t.size < 0 && t.eof {
		return t.held()
	}
	return t.size
}

// setOffset sets the offset from what the server said it has
func (t *kamplexfsTus) setOffset(header http.Header) error {
	offset, err := strconv.ParseInt(header.Get("Upload-Offset"), 10, 64)
	if err != nil {
		return fmt.Errorf("resumable upload: bad Upload-Offset: %w", err)
	}
	// Only the chunk held can be sent again
	if offset < t.start || offset > t.held() {
		return fserrors.NoRetryError(fmt.Errorf("resumable upload: server has %d bytes but %d to %d are held", offset, t.start, t.held()))
	}
	t.offset = offset
	return nil
}

// read reads more of the chunk from the source until some arrives,
// the chunk is full or the source ends or fails.
func (t *kamplexfsTus) read() {
	limit := t.chunkEnd() - t.start
	for !t.eof && t.inErr == nil && int64(len(t.buf)) < limit {
		n, err := t.in.Read(t.buf[len(t.buf):limit])
		if t.md5 != nil {
			_, _ = t.md5.Write(t.buf[len(t.buf) : len(t.buf)+n])
		}
		t.buf = t.buf[:len(t.buf)+n]
		if err == io.EOF {
			t.eof = true
		} else if err != nil {
			t.inErr = err
		}
		if n > 0 {
			return
		}
	}
}

// upload sends the data
//
// Failed requests are retried --low-level-retries times from what the
// server has, counting again each time the server has more.
func (t *kamplexfsTus) upload(ctx context.Context) error {
	retries := fs.GetConfig(ctx).LowLevelRetries
	tries, most := 0, t.offset
	resync := false // HEAD to find the offset before the next PATCH
	for {
		offset, eof := t.offset, t.eof
		var done, retry bool
		err := t.o.fs.pacer.CallNoRetry(func() (bool, error) {
			var err error
			if resync {
				err = t.head(ctx)
			} else {
				done, err = t.patch(ctx)
			}
			retry, err = t.shouldRetry(ctx, err)
			return retry, err
		})
		if t.offset > most {
			tries, most = 0, t.offset
		}
		if retry {
			tries++
			if tries < retries {
				fs.Debugf(t.o, "KamPlexFS: resumable upload: retrying at offset %d (%d/%d): %v", t.offset, tries, retries, err)
				resync = true
				continue
			}
		}
		if t.inErr != nil {
			return fmt.Errorf("failed to read data to upload: %w", t.inErr)
		}
		if errors.Is(err, errKamplexfsSessionGone) && t.final {
			// The upload may have completed with its answer lost
			if t.completed(ctx) {
				return nil
			}
			return fserrors.RetryError(err)
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if !resync && t.offset == offset && t.eof == eof {
			return fmt.Errorf("resumable upload: server is stuck at offset %d", t.offset)
		}
		resync = false
	}
}

// shouldRetry returns true if the request should be tried again
// after reading the offset with a HEAD.
func (t *kamplexfsTus) shouldRetry(ctx context.Context, err error) (bool, error) {
	if err == nil || t.inErr != nil || errors.Is(err, errKamplexfsSessionGone) {
		return false, err
	}
	if t.size >= 0 && t.eof && t.held() < t.size {
		return false, fmt.Errorf("resumable upload: source is %d bytes, expecting %d: %w", t.held(), t.size, err)
	}
	switch status := getHTTPStatusCode(err); {
	case status == http.StatusConflict:
		// An empty conflict means the offset was wrong, maybe as
		// the server hadn't finished with a PATCH which broke off.
		var apiErr *kamplexfsError
		return errors.As(err, &apiErr) && apiErr.Message == "", err
	case status >= 500:
		return true, err
	}
	return t.o.fs.shouldRetry(ctx, err)
}

// do makes a TUS request to the session
//
// It returns a *kamplexfsError for an error status, or
// errKamplexfsSessionGone for a 404.
func (t *kamplexfsTus) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Tus-Resumable", kamplexfsTusVersion)
	resp, err := t.o.fs.srv.Do(req)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			urlErr.URL = kamplexfsRedact(urlErr.URL)
		}
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return resp, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errKamplexfsSessionGone
	}
	return nil, &kamplexfsError{
		StatusCode: resp.StatusCode,
		Code:       http.StatusText(resp.StatusCode),
		Message:    strings.TrimSpace(string(body)),
	}
}

// head reads the offset of the server
func (t *kamplexfsTus) head(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, t.location, nil)
	if err != nil {
		return err
	}
	resp, err := t.do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return t.setOffset(resp.Header)
}

// patch sends the chunk from the offset of the server, returning
// true if that completes the upload.
func (t *kamplexfsTus) patch(ctx context.Context) (done bool, err error) {
	if t.offset == t.chunkEnd() && t.offset == t.held() && t.size != t.offset {
		// The server has the whole chunk so start the next
		t.start, t.buf = t.offset, t.buf[:0]
	}
	if t.size < 0 && t.offset == t.held() {
		// Find out if the data has ended before starting, so the
		// PATCH can say it completes the upload.
		t.read()
		if t.inErr != nil {
			return false, t.inErr
		}
	}
	body := &kamplexfsTusBody{t: t, pos: t.offset}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, t.location, body)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	req.Header.Set("Upload-Offset", strconv.FormatInt(t.offset, 10))
	length := t.length()
	if t.size < 0 && length >= 0 {
		// Once set it may be repeated, so send it whenever it is known
		req.Header.Set("Upload-Length", strconv.FormatInt(length, 10))
	}
	end := t.chunkEnd()
	switch {
	case t.size >= 0:
		req.ContentLength = end - t.offset
	case t.eof:
		end = t.held()
		req.ContentLength = end - t.offset
	default:
		req.ContentLength = -1
	}
	if req.ContentLength == 0 {
		req.Body = http.NoBody
	}
	t.final = t.final || (length >= 0 && end == length)
	resp, err := t.do(req)
	// Make sure the transport has stopped reading the chunk
	_ = body.Close()
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	err = t.setOffset(resp.Header)
	if err != nil {
		return false, err
	}
	total, err := strconv.ParseInt(resp.Header.Get("Upload-Length"), 10, 64)
	if err != nil || t.offset != total {
		return false, nil
	}
	t.etag = resp.Header.Get("ETag")
	t.o.kamplexfsSetBlake3FromHeader(resp.Header)
	return true, nil
}

// completed returns true if the object is stored as uploaded, for
// when the session went away before the answer to its last PATCH
// arrived.
func (t *kamplexfsTus) completed(ctx context.Context) bool {
	head, err := t.o.headObject(ctx)
	if err != nil {
		fs.Debugf(t.o, "KamPlexFS: resumable upload session gone and object not found: %v", err)
		return false
	}
	etag := strings.Trim(strings.ToLower(deref(head.ETag)), `"`)
	if deref(head.ContentLength) != t.length() || (t.md5 != nil && etag != hex.EncodeToString(t.md5.Sum(nil))) {
		fs.Debugf(t.o, "KamPlexFS: resumable upload session gone and object is size %d ETag %q", deref(head.ContentLength), etag)
		return false
	}
	fs.Debugf(t.o, "KamPlexFS: resumable upload session gone but the object is stored")
	t.offset, t.etag = t.length(), deref(head.ETag)
	t.o.kamplexfsSetBlake3(head.ResultMetadata)
	return true
}

// abort deletes the session, freeing what the server has of it
func (t *kamplexfsTus) abort(ctx context.Context) {
	// Delete it even if ctx was cancelled, but don't hold up exiting
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, t.location, nil)
	if err == nil {
		var resp *http.Response
		resp, err = t.do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
	}
	if err != nil {
		fs.Debugf(t.o, "KamPlexFS: failed to delete resumable upload: %v", err)
	}
}

// kamplexfsTusBody is the body of a PATCH
//
// It sends the chunk held from its offset and then reads the rest of
// the chunk from the source into it.
type kamplexfsTusBody struct {
	mu     sync.Mutex
	t      *kamplexfsTus
	pos    int64 // offset of the next byte to send
	closed bool
}

// Read reads the next part of the chunk
func (b *kamplexfsTusBody) Read(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t := b.t
	if b.closed {
		return 0, errors.New("resumable upload: read after close")
	}
	if b.pos == t.held() {
		t.read()
		if b.pos == t.held() {
			if t.inErr != nil {
				return 0, t.inErr
			}
			return 0, io.EOF
		}
	}
	n = copy(p, t.buf[b.pos-t.start:])
	b.pos += int64(n)
	return n, nil
}

// Close stops the body being read
func (b *kamplexfsTusBody) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return nil
}
