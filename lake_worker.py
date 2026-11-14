#!/usr/bin/env python3
"""Subprocess worker for real multi-process verification/demo of lake.

Usage: python3 lake_worker.py <db-path>

Speaks line-delimited JSON on stdin/stdout.  Emits {"ready": true} once the
store is open, then one response line per command line:

  ["begin"]              -> {"ok": null}
  ["get", k]             -> {"ok": value-or-null}
  ["put", k, v]          -> {"ok": null}
  ["delete", k]          -> {"ok": null}
  ["scan", lo, hi]       -> {"ok": [[k, v], ...]}
  ["commit"]             -> {"ok": commit-number} | {"conflict": true}
  ["abort"]              -> {"ok": null}
  ["quit"]               -> {"ok": null} then exits

Any unexpected exception -> {"error": "Type: message"}.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lake


def main():
    path = sys.argv[1]
    store = lake.Store(path)
    txn = None

    def emit(obj):
        sys.stdout.write(json.dumps(obj) + "\n")
        sys.stdout.flush()

    emit({"ready": True})
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        cmd = json.loads(line)
        op = cmd[0]
        try:
            if op == "begin":
                txn = store.begin()
                emit({"ok": None})
            elif op == "get":
                emit({"ok": txn.get(cmd[1])})
            elif op == "put":
                txn.put(cmd[1], cmd[2])
                emit({"ok": None})
            elif op == "delete":
                txn.delete(cmd[1])
                emit({"ok": None})
            elif op == "scan":
                emit({"ok": txn.scan(cmd[1], cmd[2])})
            elif op == "commit":
                try:
                    emit({"ok": txn.commit()})
                except lake.Conflict:
                    emit({"conflict": True})
                txn = None
            elif op == "abort":
                txn.abort()
                txn = None
                emit({"ok": None})
            elif op == "quit":
                emit({"ok": None})
                break
            else:
                emit({"error": "unknown op %r" % op})
        except Exception as e:  # noqa: BLE001 - reported to the driver
            emit({"error": "%s: %s" % (type(e).__name__, e)})
    store.close()


if __name__ == "__main__":
    main()
