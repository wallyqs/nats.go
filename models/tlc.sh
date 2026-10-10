#!/usr/bin/env bash
# Run TLC on one model (or all of them).
#
#   models/tlc.sh                       # every <Model>/<Model>.cfg (and <Model>/*.cfg)
#   models/tlc.sh PullConsume           # every .cfg in models/PullConsume
#   models/tlc.sh PullConsume/Bug.cfg   # one config
#
# The TLA+ tools jar is taken from $TLA2TOOLS, else downloaded once into
# models/.tools/ (git-ignored). Exit status is non-zero if any config that is
# expected to pass fails. Configs named *_bug.cfg are counterexample
# reproducers: they are EXPECTED to violate an invariant/property, and the
# script fails if TLC finds no violation for them.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
jar="${TLA2TOOLS:-$here/.tools/tla2tools.jar}"
if [[ ! -f "$jar" ]]; then
  mkdir -p "$(dirname "$jar")"
  curl -sSL -o "$jar" https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
fi

cfgs=()
if [[ $# -eq 0 ]]; then
  for d in "$here"/*/; do cfgs+=("$d"*.cfg); done
else
  for a in "$@"; do
    if [[ -d "$here/$a" ]]; then cfgs+=("$here/$a"/*.cfg); else cfgs+=("$here/$a"); fi
  done
fi

status=0
for cfg in "${cfgs[@]}"; do
  [[ -f "$cfg" ]] || continue
  dir="$(dirname "$cfg")"
  base="$(basename "$cfg" .cfg)"
  spec="$(basename "$dir")"
  # A config may name its spec in its first line: \* SPEC: Foo
  override="$(sed -n '1s/^\\\* *SPEC: *\([A-Za-z0-9_]*\).*/\1/p' "$cfg")"
  [[ -n "$override" ]] && spec="$override"
  expect_fail=0
  [[ "$base" == *_bug ]] && expect_fail=1
  printf '== %s/%s (%s) ... ' "$(basename "$dir")" "$base.cfg" "$spec"
  log="$(mktemp)"
  set +e
  (cd "$dir" && java -XX:+UseParallelGC -cp "$jar" tlc2.TLC -workers auto -deadlock \
      -config "$base.cfg" -metadir "$(mktemp -d)" "$spec.tla") >"$log" 2>&1
  rc=$?
  set -e
  # -deadlock above disables TLC's built-in deadlock check; models that care
  # about deadlock state it as an explicit invariant/property instead, so the
  # failure carries a name.
  if [[ $expect_fail -eq 1 ]]; then
    if [[ $rc -ne 0 ]] && grep -qE "(is|was) violated" "$log"; then
      echo "violation reproduced: $(grep -m1 -E "(is|was) violated" "$log" || true)"
    else
      echo "EXPECTED A VIOLATION, got none"; tail -20 "$log"; status=1
    fi
  else
    if [[ $rc -eq 0 ]]; then
      # The last "distinct states found" line is TLC's final count.
      echo "ok ($(grep 'distinct states found' "$log" | tail -1 | sed 's/.* \([0-9,]*\) distinct states found.*/\1 distinct states/'))"
    else
      echo "FAILED"; { grep -E "(is|was) violated|Error|error" "$log" || tail -20 "$log"; } | head -20; status=1
      echo "   full log: $log"
      continue
    fi
  fi
  rm -f "$log"
done
exit $status
