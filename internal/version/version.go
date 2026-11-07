package version

import (
	"fmt"
	"runtime/debug"
)

const (
	Major      = 2
	Minor      = 0
	Patch      = 0
	Prerelease = "dev.0"
)

func Base() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Patch)
}

func String() string {
	if Prerelease == "" {
		return Base()
	}
	return Base() + "-" + Prerelease
}

func Channel() string {
	return channel(Prerelease)
}

func channel(prerelease string) string {
	if prerelease == "" {
		return "stable"
	}
	for _, name := range []string{"dev", "alpha", "beta", "rc"} {
		if prerelease == name || len(prerelease) > len(name) && prerelease[:len(name)+1] == name+"." {
			return name
		}
	}
	return "prerelease"
}

// BuildInfo is provenance, never part of the application release identity.
// A nil Modified means the build did not record VCS state.
type BuildInfo struct {
	Revision      string `json:"source_revision,omitempty"`
	Modified      *bool  `json:"modified"`
	GoVersion     string `json:"go_version,omitempty"`
	ModuleVersion string `json:"module_version,omitempty"`
}

func Build() BuildInfo {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return BuildInfo{}
	}
	return buildInfo(info)
}

func buildInfo(info *debug.BuildInfo) BuildInfo {
	result := BuildInfo{GoVersion: info.GoVersion, ModuleVersion: info.Main.Version}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			result.Revision = setting.Value
		case "vcs.modified":
			if setting.Value == "true" || setting.Value == "false" {
				modified := setting.Value == "true"
				result.Modified = &modified
			}
		}
	}
	return result
}
