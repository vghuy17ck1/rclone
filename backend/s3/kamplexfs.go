// KamPlexFS provider support
//
// Everything here is only active when provider = KamPlexFS. The
// server advertises optional features through a capabilities
// endpoint and each one is only used when advertised, so the
// provider falls back to plain S3 behaviour against older servers.

package s3

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/ncw/swift/v2"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
)

const kamplexfsProvider = "KamPlexFS"

// Features the KamPlexFS server may advertise in its capabilities
const (
	kamplexfsFeatureRenameObject        = "rename-object"
	kamplexfsFeatureRenamePrefix        = "rename-prefix"
	kamplexfsFeatureSequentialMultipart = "sequential-multipart"
	kamplexfsFeatureMtimeNanos          = "mtime-nanos"
	kamplexfsFeatureChecksums           = "checksums"
)

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
}}

// kamplexfsOpt holds the provider specific options
type kamplexfsOpt struct {
	SequentialUpload bool `config:"kamplexfs_sequential_upload"`
	ExactModTime     bool `config:"kamplexfs_exact_modtime"`
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
}

// has returns true if the server advertised the feature
func (k *kamplexfs) has(feature string) bool {
	return k.features[feature]
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
	// ETag, so the HEAD after each upload adds nothing.
	if !optionIsSet(m, "no_head") {
		f.opt.NoHead = true
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
	creds, err := c.Credentials.Retrieve(ctx)
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
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
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
