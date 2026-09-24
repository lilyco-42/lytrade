#!/usr/bin/env python3
"""Idempotently add lytrade upstream+location into /etc/pingap.toml (with backup)."""
import shutil
import sys

CFG = "/etc/pingap.toml"
text = open(CFG, encoding="utf-8").read()

if "[locations.lytrade]" in text:
    print("already configured, nothing to do")
    sys.exit(0)

shutil.copy(CFG, CFG + ".bak-lytrade")

if "[upstreams.lytrade_backend]" not in text:
    text = text.replace(
        "[upstreams.placeholder]",
        '[upstreams.lytrade_backend]\naddrs = ["127.0.0.1:8792"]\n\n[upstreams.placeholder]',
        1,
    )
if '"lytrade"' not in text:
    marker = '    "lysource",\n'
    if marker not in text:
        print("ERROR: locations marker not found", file=sys.stderr)
        sys.exit(1)
    text = text.replace(marker, marker + '    "lytrade",\n', 1)

text += """
[locations.lytrade]
enable_reverse_proxy_headers = true
path = "/lytrade"
rewrite = "^/lytrade/?(.*)$ /$1"
upstream = "lytrade_backend"
"""

open(CFG, "w", encoding="utf-8").write(text)
print("patched ok")
