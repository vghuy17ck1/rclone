// Package api has type definitions for the KamPlexFS JSON REST API
package api

import (
	"fmt"
	"regexp"
	"time"
)

// Resource kinds
const (
	KindFile       = "kamplexfs#file"
	KindFolder     = "kamplexfs#folder"
	KindFileList   = "kamplexfs#fileList"
	KindBucketList = "kamplexfs#bucketList"
	KindBucket     = "kamplexfs#bucket"
)

// File is a FileResource
type File struct {
	Kind         string            `json:"kind"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	Path         string            `json:"path"`
	ParentPath   string            `json:"parentPath"`
	Size         int64             `json:"size"`
	MimeType     string            `json:"mimeType"`
	MD5Checksum  string            `json:"md5Checksum"`
	CreatedTime  string            `json:"createdTime"`
	ModifiedTime string            `json:"modifiedTime"`
	CreatedAt    int64             `json:"createdAt"`
	UpdatedAt    int64             `json:"updatedAt"`
	CustomTags   map[string]string `json:"customTags,omitempty"`
}

// Folder is a FolderResource
type Folder struct {
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Path       string `json:"path"`
	ParentPath string `json:"parentPath"`
}

// List is a ListResponse
type List struct {
	Kind          string   `json:"kind"`
	Path          string   `json:"path"`
	Files         []File   `json:"files"`
	Folders       []Folder `json:"folders"`
	PageSize      int      `json:"pageSize"`
	Offset        int      `json:"offset"`
	NextPageToken string   `json:"nextPageToken,omitempty"`
	HasMore       bool     `json:"hasMore"`
	// Recursive is set on recursive listings by servers which put
	// every folder below the prefix in the first page
	Recursive bool `json:"recursive,omitempty"`
}

// BucketList is the response to listing the buckets
type BucketList struct {
	Kind    string   `json:"kind"`
	Buckets []string `json:"buckets"`
}

// Bucket is a BucketResource
type Bucket struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// CreateBucket is the request to create a bucket
type CreateBucket struct {
	Name string `json:"name"`
}

// Item is the response to a request which may return a file, a
// folder or a folder listing. Kind says which.
type Item struct {
	File
	Files   []File   `json:"files"`
	Folders []Folder `json:"folders"`
}

// CreateFolder is the request to create a folder
type CreateFolder struct {
	Path string `json:"path"`
}

// MoveCopy is the request to move or copy a file or, when From ends
// in "/", everything under a prefix.
type MoveCopy struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Delete is the request to delete files or, for targets ending in
// "/", everything under a prefix.
type Delete struct {
	Targets []string `json:"targets"`
}

// DeleteResult is the response to a Delete
type DeleteResult struct {
	Kind         string   `json:"kind"`
	Targets      []string `json:"targets"`
	DeletedCount int64    `json:"deletedCount"`
}

// SetModTime is the request to set the modification time of a file
type SetModTime struct {
	ModifiedTime string `json:"modifiedTime"`
}

// Error is an error returned by the server
//
// Most errors are a JSON envelope but authentication failures are
// plain text which is kept in Text.
type Error struct {
	Details struct {
		Code    int    `json:"code"`
		Status  string `json:"status"`
		Message string `json:"message"`
	} `json:"error"`
	StatusCode int    `json:"-"` // HTTP status code of the response
	Text       string `json:"-"` // plain text body if not JSON
}

// Error satisfies the error interface
func (e *Error) Error() string {
	if e.Details.Status != "" || e.Details.Message != "" {
		return fmt.Sprintf("kamplexfs: HTTP %d %s: %s", e.StatusCode, e.Details.Status, e.Details.Message)
	}
	return fmt.Sprintf("kamplexfs: HTTP %d: %s", e.StatusCode, e.Text)
}

var matchMD5 = regexp.MustCompile(`^[0-9a-f]{32}$`)

// MD5 returns the MD5 of the file or "" if the server hasn't hashed
// it yet.
func (f *File) MD5() string {
	if matchMD5.MatchString(f.MD5Checksum) {
		return f.MD5Checksum
	}
	return ""
}

// ModTime returns the modification time of the file and whether it
// carries a sub-second part.
func (f *File) ModTime() (t time.Time, hasFraction bool, err error) {
	t, err = time.Parse(time.RFC3339Nano, f.ModifiedTime)
	if err != nil {
		return t, false, err
	}
	return t, t.Nanosecond() != 0, nil
}
