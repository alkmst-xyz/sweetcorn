package app

import (
	"fmt"
	"net/http"
	"strings"
)

//-----------------------------------------------------------------------------
// Magic used to enable SPA mode for sweetcorn web app.
// Ref: https://hackandsla.sh/posts/2021-11-06-serve-spa-from-go/
//
// TODO:
//
// 1. revise FS implementation used, inner working, etc. when a 404 error is
// generated. This enables proper SPA functionality.
// 2. implement similar logic as `try_files` from nginx.
//-----------------------------------------------------------------------------

// Note: The file server is explicitly configured to respond with the root index.
func newSPAFileServer(webAssetsDirPath string) http.Handler {
	webAssetsDir := http.Dir(webAssetsDirPath)

	webFileServer := http.FileServer(webAssetsDir)
	webServeIndex := serveFileContents("index.html", webAssetsDir)

	return intercept404(webFileServer, webServeIndex)
}

// See https://stackoverflow.com/questions/26141953/custom-404-with-gorilla-mux-and-std-http-fileserver
func intercept404(handler, on404 http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hookedWriter := &hookedResponseWriter{ResponseWriter: w}
		handler.ServeHTTP(hookedWriter, r)

		if hookedWriter.got404 {
			on404.ServeHTTP(w, r)
		}
	})
}

type hookedResponseWriter struct {
	http.ResponseWriter
	got404 bool
}

func (hrw *hookedResponseWriter) WriteHeader(status int) {
	if status == http.StatusNotFound {
		// Don't actually write the 404 header, just set a flag.
		hrw.got404 = true
	} else {
		hrw.ResponseWriter.WriteHeader(status)
	}
}

func (hrw *hookedResponseWriter) Write(p []byte) (int, error) {
	if hrw.got404 {
		// No-op, but pretend that we wrote len(p) bytes to the writer.
		return len(p), nil
	}

	return hrw.ResponseWriter.Write(p)
}

func serveFileContents(file string, files http.FileSystem) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Restrict only to instances where the browser is looking for an HTML file
		if !strings.Contains(r.Header.Get("Accept"), "text/html") {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "404 not found")

			return
		}

		// Open the file and return its contents using http.ServeContent
		index, err := files.Open(file)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}

		fi, err := index.Stat()
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s not found", file)

			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, fi.Name(), fi.ModTime(), index)
	}
}
