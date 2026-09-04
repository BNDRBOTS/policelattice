#!/usr/bin/env python3
"""Probe every configured source and print what it actually returns.

Run this on any machine with outbound internet access:

    python -m scripts.verify_sources
    python -m scripts.verify_sources --source phoenix_ckan_uof --rows 3

It performs real requests and prints the HTTP status, the row count the
source advertises, and one sample row per source so you can see the real
column names with your own eyes. Nothing is cached, stubbed or simulated.
"""

from __future__ import annotations

import argparse
import json
import sys

from app.pipeline.registry import load_catalog


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="probe only this source id")
    parser.add_argument("--rows", type=int, default=1, help="sample rows to print")
    args = parser.parse_args()

    catalog = load_catalog()
    if args.source:
        catalog = [s for s in catalog if s.id == args.source]
        if not catalog:
            print(f"no such source: {args.source}", file=sys.stderr)
            return 2

    reachable = 0
    for definition in catalog:
        adapter = definition.build()
        print(f"\n{'=' * 78}\n{definition.id}  ({definition.adapter})\n{definition.name}")
        try:
            verification = adapter.verify()
        except Exception as exc:  # noqa: BLE001
            print(f"  VERIFY ERROR  {type(exc).__name__}: {exc}")
            continue

        reachable += int(verification.ok)
        print(f"  ok            {verification.ok}")
        print(f"  http_status   {verification.http_status}")
        print(f"  rows_advertised {verification.rows_total_reported}")
        print(f"  verified_at   {verification.verified_at}")
        if verification.detail:
            print(f"  detail        {verification.detail}")
        if verification.error:
            print(f"  error         {verification.error}")
        if not verification.ok:
            continue

        shown = 0
        try:
            for page in adapter.fetch(months_back=1):
                print(f"  page          {len(page.rows)} rows  <- {page.url}")
                for row in page.rows[: args.rows]:
                    print("  sample        " + json.dumps(row, default=str)[:400])
                shown += len(page.rows)
                if shown >= args.rows * 3:
                    break
        except Exception as exc:  # noqa: BLE001
            print(f"  FETCH ERROR   {type(exc).__name__}: {exc}")

    print(f"\n{'=' * 78}\n{reachable}/{len(catalog)} sources reachable")
    return 0 if reachable else 1


if __name__ == "__main__":
    sys.exit(main())
