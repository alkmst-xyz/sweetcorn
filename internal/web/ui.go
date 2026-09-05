package web

import (
	"embed"
	"io/fs"
)

// Embed files from `dist/`, excluding files starting with '.' or '_'.
//
//go:embed all:dist
var assets embed.FS

// FS contains the web UI assets.
func WebAssetsFS() (fs.FS, error) {
	return fs.Sub(assets, "dist")
}
