#!/usr/bin/env python3
"""lake demo: snapshot-isolated multi-batch reads/writes on SQLite.

Walks through the API single-process, then shows two real OS processes
interleaving transactions on the same database file.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import lake


def section(title):
    print("\n=== %s ===" % title)


def main():
    tmp = tempfile.mkdtemp(prefix="lake-demo-")
    path = os.path.join(tmp, "demo.db")
    try:
        print("lake.Store(%r)  -- initially empty repository" % path)
        store = lake.Store(path)

        section("1. one batch: put / get / scan / delete, then commit")
        t = store.begin()
        t.put(1, "apple")
        t.put(2, "banana")
        t.put(15, "fig")
        print("  own scan [0,20) before commit:", t.scan(0, 20))
        t.delete(2)
        print("  after delete(2): get(2) ->", t.get(2))
        print("  committed as commit number", t.commit())

        section("2. snapshot isolation: later commits don't disturb readers")
        a = store.begin()
        b = store.begin()
        b.put(1, "apricot")
        print("  B commits put(1,'apricot') as", b.commit())
        print("  A (began earlier) still reads get(1) ->", a.get(1))
        a.abort()

        section("3. conflict: A scanned [10,20), B wrote 15 meanwhile")
        a = store.begin()
        b = store.begin()
        print("  A scan [10,20) ->", a.scan(10, 20))
        b.put(15, "FIG")
        print("  B commits put(15,'FIG') as", b.commit())
        a.put(99, "x")
        try:
            a.commit()
            print("  UNEXPECTED: A committed")
        except lake.Conflict as e:
            print("  A commit -> lake.Conflict: %s (whole batch discarded)" % e)
        t = store.begin()
        print("  get(99) after conflict ->", t.get(99),
              " (A's write was not published)")
        t.abort()

        section("4. no intersection -> both commit")
        a = store.begin()
        b = store.begin()
        a.put(1, "A-writes-1")
        b.put(2, "B-writes-2")
        print("  A commits as", a.commit(), "; B commits as", b.commit())

        section("5. abort publishes nothing")
        a = store.begin()
        a.put(50, "ghost")
        a.abort()
        t = store.begin()
        print("  get(50) after abort ->", t.get(50))
        t.abort()

        section("6. handles die after commit/abort")
        t = store.begin()
        t.commit()
        try:
            t.get(1)
        except RuntimeError as e:
            print("  get after commit -> RuntimeError: %s" % e)

        section("7. reopen keeps committed data")
        store.close()
        store = lake.Store(path)
        t = store.begin()
        print("  scan [-100,100) after reopen:", t.scan(-100, 100))
        t.abort()
        store.close()

        section("8. two OS processes interleaved on the same file")
        worker = os.path.join(HERE, "lake_worker.py")
        pa = subprocess.Popen([sys.executable, "-u", worker, path],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True)
        pb = subprocess.Popen([sys.executable, "-u", worker, path],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              text=True)

        def cmd(p, *c):
            p.stdin.write(json.dumps(c) + "\n")
            p.stdin.flush()
            return json.loads(p.stdout.readline())

        pa.stdout.readline()   # ready
        pb.stdout.readline()   # ready
        print("  P1 begin:", cmd(pa, "begin"), " P2 begin:", cmd(pb, "begin"))
        print("  P1 scan [10,20):", cmd(pa, "scan", 10, 20))
        cmd(pb, "put", 15, "from-P2")
        print("  P2 put(15) commit:", cmd(pb, "commit"))
        cmd(pa, "put", 1, "from-P1")
        print("  P1 put(1) commit:", cmd(pa, "commit"),
              " <- conflict detected across processes")
        cmd(pa, "begin")
        cmd(pb, "begin")
        cmd(pa, "put", 1000, "P1")
        cmd(pb, "put", 2000, "P2")
        print("  disjoint writes -> P1 commit:", cmd(pa, "commit"),
              " P2 commit:", cmd(pb, "commit"))
        cmd(pa, "quit")
        cmd(pb, "quit")
        pa.wait()
        pb.wait()

        store = lake.Store(path)
        t = store.begin()
        print("  final scan [-2000,2001):", t.scan(-2000, 2001))
        t.abort()
        store.close()

        print("\nDEMO OK")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
