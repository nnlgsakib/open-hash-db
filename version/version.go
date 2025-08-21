package version

var (
	Version = "unknown" // to be set at build time from git tag
	Author  = "unknown" // to be set at build time from git config
	Branch  = "unknown" // to be set at build time from git branch
)
