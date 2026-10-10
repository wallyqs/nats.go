#!/usr/bin/env bash
# Run the model-derived tests.
#
#   modeltest/run.sh                     # all packages, -race
#   modeltest/run.sh ./pullconsume/...   # one package
#   MODELTEST_KNOWN_BUGS=1 modeltest/run.sh   # also run the known-bug reproducers
#
# zizek.go is resolved through the replace directive in go.mod, which points at
# a sibling checkout of github.com/wallyqs/zizek (../../zizek/packages/go).
# Set ZIZEK_DIR to use another checkout; if neither exists, it is cloned into
# modeltest/.zizek (git-ignored).
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here"
zdir="${ZIZEK_DIR:-}"
if [[ -z "$zdir" ]]; then
  if [[ -f "$here/../../zizek/packages/go/go.mod" ]]; then
    zdir="$here/../../zizek/packages/go"
  else
    if [[ ! -d "$here/.zizek" ]]; then
      git clone --depth 1 https://github.com/wallyqs/zizek "$here/.zizek"
    fi
    zdir="$here/.zizek/packages/go"
  fi
fi
zdir="$(cd "$zdir" && pwd)"
flags=(-modfile="$here/go.mod")
if [[ "$zdir" != "$(cd "$here/../../zizek/packages/go" 2>/dev/null && pwd)" ]]; then
  # Point the replace at the chosen checkout through a temporary modfile so
  # go.mod itself is never rewritten.
  tmp="$(mktemp -d)"
  cp go.mod "$tmp/go.mod"; cp go.sum "$tmp/go.sum"
  go mod edit -modfile="$tmp/go.mod" -replace=github.com/wallyqs/zizek.go="$zdir"
  flags=(-modfile="$tmp/go.mod")
fi
pkgs=("$@")
[[ ${#pkgs[@]} -eq 0 ]] && pkgs=(./...)
go test "${flags[@]}" -race -count=1 "${pkgs[@]}"
