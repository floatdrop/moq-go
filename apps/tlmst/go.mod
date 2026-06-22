module github.com/floatdrop/moq-go/apps/tlmst

go 1.26.4

// The parent module is developed in-tree; resolve it locally. The go.work
// file already does this for workspace builds, but the replace keeps plain
// `go` commands (mod tidy, docker builds) working outside the workspace too.
replace github.com/floatdrop/moq-go => ../..

require (
	github.com/floatdrop/moq-go v0.0.0-00010101000000-000000000000
	github.com/quic-go/quic-go v0.60.0
	github.com/wailsapp/wails/v3 v3.0.0-alpha2.105
)

require (
	github.com/adrg/xdg v0.5.3 // indirect
	github.com/coder/websocket v1.8.14 // indirect
	github.com/ebitengine/purego v0.10.1 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/godbus/dbus/v5 v5.2.2 // indirect
	github.com/jchv/go-winloader v0.0.0-20250406163304-c1995be93bd1 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/wailsapp/wails/webview2 v1.0.24 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
)
