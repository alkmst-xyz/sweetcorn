package web

import (
	"embed"
	"io/fs"
)

// Embed files from `build/`, excluding files starting with '.' or '_'.
//
//go:embed all:build
var assets embed.FS

// FS contains the web UI assets.
func WebAssetsFS() (fs.FS, error) {
	return fs.Sub(assets, "build")
}
