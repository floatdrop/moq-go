{ pkgs, lib, config, ... }:

{
  # https://devenv.sh/basics/
  # A reproducible Go development environment for moq-go.

  # https://devenv.sh/languages/
  # Provides the Go toolchain (matching go.mod's `go 1.27`) plus go env wiring.
  languages.go.enable = true;

  # https://devenv.sh/packages/
  # Tooling the repo expects on PATH. The lint config (.golangci.yml) enables the
  # `goimports` and `golines` formatters, so both must be available.
  packages = with pkgs; [
    # Must be built with go1.27 or newer: an older binary refuses a go1.27
    # target with "the Go language version ... is lower than the targeted
    # Go version". If nixpkgs still ships a go1.26 build, install it with
    # `GOTOOLCHAIN=go1.27.0 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.0`.
    golangci-lint   # `golangci-lint run` — see .golangci.yml
    golines         # long-line formatter enabled in .golangci.yml
    gotools         # provides goimports
    gopls           # language server
    delve           # `dlv` debugger
    gnumake         # the Makefile drives build/test/interop targets
  ];

  # https://devenv.sh/scripts/
  # Thin wrappers around the canonical commands from CLAUDE.md / the Makefile.
  scripts.build.exec = "go build ./...";
  scripts.test.exec = "go test ./...";
  scripts.test-race.exec = "go test -race ./...";
  scripts.lint.exec = "golangci-lint run";
  scripts.bench.exec = "go test -run='^$' -bench=. -benchmem ./...";
  # modernize check (errorsastype) — see CLAUDE.md. golangci-lint v2.13.0
  # bundles the analyzer, so `lint` covers it; this runs @latest to catch
  # analyzers newer than the bundled snapshot. Pass -fix to apply.
  scripts.modernize.exec = ''
    go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest \
      -errorsastype "$@" ./...
  '';

  enterShell = ''
    echo "moq-go devenv — go $(go version | cut -d' ' -f3 | sed 's/go//')"
    echo "  scripts: build | test | test-race | lint | bench | modernize"
  '';

  # https://devenv.sh/tests/
  # `devenv test` sanity-checks the toolchain wiring.
  enterTest = ''
    go version
    golangci-lint version
  '';
}
