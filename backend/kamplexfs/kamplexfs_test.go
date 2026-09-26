// Test KamPlexFS filesystem interface
package kamplexfs

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against a real server
//
// Configure it as the remote TestKamPlexFS: with a bucket, e.g.
//
//	go test -v -remote TestKamPlexFS:bucket
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestKamPlexFS:",
		NilObject:  (*Object)(nil),
	})
}

// runFake runs the integration tests against the fake server
func runFake(t *testing.T, name string, fake *fakeServer) {
	if *fstest.RemoteName != "" {
		t.Skip("skipping as -remote is set")
	}
	const secret = "fake secret"
	fake.secret = []byte(secret)
	fake.buckets["bucket"] = true
	srv := httptest.NewServer(fake)
	defer srv.Close()

	remote := "TestKamPlexFSFake" + name
	prefix := "RCLONE_CONFIG_" + strings.ToUpper(remote) + "_"
	t.Setenv(prefix+"TYPE", "kamplexfs")
	t.Setenv(prefix+"URL", srv.URL)
	t.Setenv(prefix+"JWT_SECRET", obscure.MustObscure(secret))
	// use small pages to test paging
	t.Setenv(prefix+"LIST_CHUNK", "7")

	// With the bucket endpoints the tests make their own bucket so
	// the bucket level tests run too.
	root := ":"
	if !fake.bucketAPI {
		root = ":bucket"
	}
	fstests.Run(t, &fstests.Opt{
		RemoteName:  remote + root,
		NilObject:   (*Object)(nil),
		QuickTestOK: true,
	})
}

// TestFakeDirectFS tests against a server in direct-fs mode
func TestFakeDirectFS(t *testing.T) {
	runFake(t, "DirectFS", newFakeServer(""))
}

// TestFakePackedVolume tests against a server in packed-volume mode
// which can't store empty folders
func TestFakePackedVolume(t *testing.T) {
	fake := newFakeServer("")
	fake.packed = true
	runFake(t, "PackedVolume", fake)
}

// TestFakeOldServer tests against a server without sub-second
// mtimes, set mtime, recursive listing, 404 for a missing source or
// bucket endpoints
func TestFakeOldServer(t *testing.T) {
	runFake(t, "OldServer", newFakeServer("").oldServer())
}
