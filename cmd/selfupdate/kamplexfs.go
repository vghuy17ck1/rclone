//go:build !noselfupdate

package selfupdate

// selfupdate downloads official rclone, which would replace this
// KamPlexFS build and lose its KamPlexFS support, so it refuses to
// unless asked to with --force-upstream-update.

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/rclone/rclone/fs/config/flags"
)

// kamplexfsReleasesURL is where the KamPlexFS builds are released
const kamplexfsReleasesURL = "https://github.com/vghuy17ck1/rclone/releases"

// forceUpstreamUpdate is set by --force-upstream-update
var forceUpstreamUpdate bool

// kamplexfsOutput is where the warnings are written
var kamplexfsOutput io.Writer = os.Stderr

const kamplexfsWarning = `WARNING: this is the KamPlexFS build of rclone. selfupdate installs
official upstream rclone, which would replace this build and remove
the KamPlexFS support (provider = KamPlexFS in s3 and type = kamplexfs).
Get KamPlexFS builds from ` + kamplexfsReleasesURL

const kamplexfsHelp = `### KamPlexFS build

This build of rclone has KamPlexFS support which official rclone
doesn't have, so selfupdate refuses to replace it unless given
` + "`--force-upstream-update`" + `. ` + "`--check`" + ` works without it but the
versions it reports are upstream rclone releases. Get KamPlexFS builds
from ` + kamplexfsReleasesURL + `.`

func init() {
	flags.BoolVarP(cmdSelfUpdate.Flags(), &forceUpstreamUpdate, "force-upstream-update", "", false, "Replace this KamPlexFS build with official upstream rclone", "")
	cmdSelfUpdate.Long += "\n\n" + kamplexfsHelp
}

// kamplexfsCheckNote says the versions --check reports are upstream
// releases
func kamplexfsCheckNote() {
	_, _ = fmt.Fprintf(kamplexfsOutput, "Note: this is the KamPlexFS build of rclone. The versions below are\nupstream rclone releases, not KamPlexFS builds - see %s\n", kamplexfsReleasesURL)
}

// kamplexfsGuard returns an error unless opt may replace this build
// with upstream rclone, warning that it would.
func kamplexfsGuard(opt *Options) error {
	if opt.Check {
		kamplexfsCheckNote()
		return nil
	}
	_, _ = fmt.Fprintln(kamplexfsOutput, kamplexfsWarning)
	if !forceUpstreamUpdate {
		return errors.New("refusing to replace the KamPlexFS build with upstream rclone - use --force-upstream-update to do it anyway")
	}
	return nil
}
