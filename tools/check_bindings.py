#!/usr/bin/env python3
"""Fails if the Pascal unit and the Rust crate have drifted apart.

Zpr.pas binds every symbol THREE times — the dynamic pointer table, the FPC
static externals and the Delphi static externals — and a symbol added to Rust
reaches Pascal only if all three are updated. Nothing about that is enforced by
either compiler: a missing binding is a link error at best and, in the dynamic
case, a runtime "missing symbol" on the one call path nobody exercised.

    python3 tools/check_bindings.py     # exit 0 = in step
"""
import glob, re, sys

rust = set()
for f in glob.glob("crates/zpr/src/*.rs"):
    rust |= set(re.findall(r'pub extern "C" fn (zpr_[a-z0-9_]+)', open(f).read()))

pas = open("pascal/Zpr.pas").read()
paths = {
    "dynamic (Resolve)":   set(re.findall(r"Resolve\('(zpr_[a-z0-9_]+)'\)", pas)),
    "FPC static":          set(re.findall(r"external name '(zpr_[a-z0-9_]+)'", pas)),
    "Delphi static":       set(re.findall(r"external ZPR_LIB name '(zpr_[a-z0-9_]+)'", pas)),
}

bad = False
for label, bound in paths.items():
    missing, stale = sorted(rust - bound), sorted(bound - rust)
    if missing or stale:
        bad = True
        print(f"{label}:")
        for m in missing: print(f"  MISSING  {m}  (exported by Rust, unreachable from Pascal)")
        for s in stale:   print(f"  STALE    {s}  (bound in Pascal, no such Rust export)")

print(f"{len(rust)} exports, {'DRIFTED' if bad else 'all three binding paths in step'}")
sys.exit(1 if bad else 0)
