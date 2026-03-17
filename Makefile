GO ?= F:/sdk/go/bin/go.exe
APP := kafka-redirect
PKG := ./cmd/kafka-redirect

WIN_OUT := $(APP)-windows-amd64.exe
LINUX_OUT := $(APP)-linux-amd64

.PHONY: all windows linux clean

all: windows linux

windows: export GOOS = windows
windows: export GOARCH = amd64
windows:
	"$(GO)" build -o "$(WIN_OUT)" "$(PKG)"

linux: export GOOS = linux
linux: export GOARCH = amd64
linux:
	"$(GO)" build -o "$(LINUX_OUT)" "$(PKG)"

clean:
	powershell -NoProfile -Command "Remove-Item -Force -ErrorAction SilentlyContinue '$(WIN_OUT)','$(LINUX_OUT)' | Out-Null; exit 0"
