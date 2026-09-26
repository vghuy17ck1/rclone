//go:build !noselfupdate

package selfupdate

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain lets the upstream tests update the binary as they expect
// to, the tests below set --force-upstream-update themselves.
func TestMain(m *testing.M) {
	forceUpstreamUpdate = true
	kamplexfsOutput = io.Discard
	os.Exit(m.Run())
}

// setupKamPlexFS captures the warnings, sets --force-upstream-update
// to force and the running version to version.
func setupKamPlexFS(t *testing.T, force bool, version string) *bytes.Buffer {
	var out bytes.Buffer
	oldOutput, oldForce, oldVersion := kamplexfsOutput, forceUpstreamUpdate, fs.Version
	kamplexfsOutput, forceUpstreamUpdate, fs.Version = &out, force, version
	t.Cleanup(func() {
		kamplexfsOutput, forceUpstreamUpdate, fs.Version = oldOutput, oldForce, oldVersion
	})
	return &out
}

// The versions are given and --beta set so GetVersion doesn't need
// the network.

func TestKamPlexFSSelfUpdateRefused(t *testing.T) {
	ctx := context.Background()
	out := setupKamPlexFS(t, false, "v1.0.0")
	path := filepath.Join(t.TempDir(), "rclone")

	err := InstallUpdate(ctx, &Options{Beta: true, Version: "v1.0.1", Output: path})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--force-upstream-update")
	assert.Contains(t, out.String(), "WARNING: this is the KamPlexFS build")
	assert.Contains(t, out.String(), "official upstream rclone")
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "must not download anything")
}

func TestKamPlexFSSelfUpdateForced(t *testing.T) {
	ctx := context.Background()
	out := setupKamPlexFS(t, true, "v1.0.1")
	path := filepath.Join(t.TempDir(), "rclone")

	// It gets as far as finding it is already up to date
	require.NoError(t, InstallUpdate(ctx, &Options{Beta: true, Version: "v1.0.1", Output: path}))
	assert.Contains(t, out.String(), "WARNING: this is the KamPlexFS build")
}

func TestKamPlexFSSelfUpdateCheck(t *testing.T) {
	ctx := context.Background()
	out := setupKamPlexFS(t, false, "v1.0.0")
	path := filepath.Join(t.TempDir(), "rclone")

	require.NoError(t, InstallUpdate(ctx, &Options{Check: true, Beta: true, Version: "v1.0.1", Output: path}))
	assert.Contains(t, out.String(), "upstream rclone releases, not KamPlexFS builds")
	assert.NotContains(t, out.String(), "WARNING")
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "must not download anything")
}
