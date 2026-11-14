#!/usr/bin/env python3
"""End-to-end verification for lake.py.

  1. scripted semantic checks (single process, multiple handles)
  2. deterministic randomized interleavings cross-checked against an
     independent in-memory reference model (refmodel.py)
  3. real multi-process interleavings driven through subprocess workers
     (lake_worker.py), scripted and randomized, plus persistence across
     close/reopen

Exits 0 and prints VERIFY OK on success; raises on the first mismatch.
"""

import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import lake
import refmodel

INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def assert_runtime_error(fn, what):
    try:
        fn()
    except RuntimeError:
        return
    except Exception as e:
        raise AssertionError("%s: expected RuntimeError, got %s: %s"
                             % (what, type(e).__name__, e))
    raise AssertionError("%s: expected RuntimeError, nothing raised" % what)


def assert_conflict(fn, what):
    try:
        fn()
    except lake.Conflict:
        return
    except Exception as e:
        raise AssertionError("%s: expected lake.Conflict, got %s: %s"
                             % (what, type(e).__name__, e))
    raise AssertionError("%s: expected lake.Conflict, nothing raised" % what)


# ---------------------------------------------------------------------------
# 1. scripted checks
# ---------------------------------------------------------------------------

def test_basics(tmp):
    store = lake.Store(os.path.join(tmp, "basics.db"))
    t = store.begin()
    check(t.get(5) is None, "get on empty store must be None")
    check(t.scan(10, 20) == [], "scan on empty store must be []")
    n0 = t.commit()
    check(isinstance(n0, int), "empty commit returns a commit number")

    t = store.begin()
    t.put(1, "one")
    t.put(2, "two")
    t.put(15, "fifteen")
    check(t.get(1) == "one", "read-your-write")
    t.delete(2)
    check(t.get(2) is None, "read-your-delete")
    check(t.scan(-10, 20) == [(1, "one"), (15, "fifteen")],
          "scan overlays own writes/deletes")
    n1 = t.commit()
    check(n1 > n0, "commit numbers increase")

    t = store.begin()
    check(t.get(1) == "one" and t.get(2) is None and t.get(15) == "fifteen",
          "committed state readable")
    check(t.scan(1, 16) == [(1, "one"), (15, "fifteen")], "committed scan")
    check(t.scan(2, 15) == [], "scan excludes deleted keys, half-open range")
    n2 = t.commit()
    check(n2 > n1, "commit numbers strictly increase")

    t = store.begin()
    t.put(7, "seven")
    t.delete(1)
    t.abort()
    t = store.begin()
    check(t.get(7) is None and t.get(1) == "one", "abort publishes nothing")
    t.abort()
    store.close()


def test_snapshot(tmp):
    store = lake.Store(os.path.join(tmp, "snapshot.db"))
    t = store.begin()
    t.put(1, "one")
    t.put(15, "fifteen")
    t.commit()

    a = store.begin()          # snapshot taken here
    b = store.begin()
    b.put(1, "uno")
    b.delete(15)
    b.put(100, "x")
    b.commit()

    check(a.get(1) == "one", "snapshot: get unchanged by later commit")
    check(a.get(15) == "fifteen", "snapshot: later delete invisible")
    check(a.get(100) is None, "snapshot: later insert invisible")
    check(a.scan(0, 200) == [(1, "one"), (15, "fifteen")],
          "snapshot: scan stable")
    a.abort()
    store.close()


def test_conflicts(tmp):
    # spec example: A scans empty [10,20), B writes 15 and commits, A conflicts
    store = lake.Store(os.path.join(tmp, "conflict1.db"))
    a = store.begin()
    b = store.begin()
    check(a.scan(10, 20) == [], "A scans empty range")
    b.put(15, "B15")
    b.commit()
    a.put(1, "A1")
    assert_conflict(a.commit,
                    "A must conflict: scan [10,20) intersects B's write of 15")
    t = store.begin()
    check(t.get(1) is None and t.get(15) == "B15",
          "conflicted batch is not published")
    t.abort()
    store.close()

    # scan range boundaries: [10,20) -- 20 and 9 outside, 10 and 19 inside
    for key, must_conflict in [(20, False), (9, False), (10, True), (19, True)]:
        store = lake.Store(os.path.join(tmp, "bound-%d.db" % key))
        a = store.begin()
        b = store.begin()
        a.scan(10, 20)
        b.put(key, "v")
        b.commit()
        a.put(1, "x")
        if must_conflict:
            assert_conflict(a.commit,
                            "write at %d must conflict with scan [10,20)" % key)
        else:
            a.commit()
        store.close()

    # disjoint writes both succeed (spec example)
    store = lake.Store(os.path.join(tmp, "disjoint.db"))
    a = store.begin()
    b = store.begin()
    a.put(1, "A1")
    b.put(2, "B2")
    na = a.commit()
    nb = b.commit()
    check(nb > na, "commit numbers strictly increasing across transactions")
    store.close()

    # write-write on same key conflicts
    store = lake.Store(os.path.join(tmp, "ww.db"))
    a = store.begin()
    b = store.begin()
    a.put(5, "a")
    b.put(5, "b")
    a.commit()
    assert_conflict(b.commit, "write-write on same key must conflict")
    store.close()

    # point-read key written by the other -> conflict
    store = lake.Store(os.path.join(tmp, "rw.db"))
    t = store.begin()
    t.put(2, "two")
    t.commit()
    a = store.begin()
    b = store.begin()
    check(a.get(2) == "two", "A point-reads 2")
    b.put(2, "TWO")
    b.commit()
    a.put(3, "three")
    assert_conflict(a.commit, "A read key 2 which B overwrote")
    store.close()

    # same-value write still counts as a write
    store = lake.Store(os.path.join(tmp, "samevalue.db"))
    t = store.begin()
    t.put(7, "x")
    t.commit()
    a = store.begin()
    b = store.begin()
    check(a.get(7) == "x", "A reads 7")
    b.put(7, "x")          # same value
    b.commit()
    a.put(8, "y")
    assert_conflict(a.commit, "same-value write is still a write")
    store.close()

    # deleting a missing key still counts as a write
    store = lake.Store(os.path.join(tmp, "delempty.db"))
    a = store.begin()
    b = store.begin()
    check(a.get(9) is None, "A reads missing key 9")
    b.delete(9)            # key does not exist
    b.commit()
    a.put(1, "x")
    assert_conflict(a.commit, "delete of missing key is still a write")
    store.close()

    # reads/writes outside the read set and scan ranges do not conflict
    store = lake.Store(os.path.join(tmp, "noconflict.db"))
    a = store.begin()
    b = store.begin()
    a.get(1)
    a.scan(10, 20)
    b.get(2)
    b.scan(30, 40)
    b.put(25, "z")
    b.put(20, "boundary")   # 20 is the exclusive hi of A's scan
    b.commit()
    a.put(3, "c")
    a.commit()
    store.close()


def test_invalidation(tmp):
    store = lake.Store(os.path.join(tmp, "invalid.db"))

    def ops(t):
        return [lambda: t.get(1), lambda: t.put(1, "x"), lambda: t.delete(1),
                lambda: t.scan(0, 1), lambda: t.commit(), lambda: t.abort()]

    t = store.begin()
    t.abort()
    for op in ops(t):
        assert_runtime_error(op, "op after abort")

    t = store.begin()
    t.commit()
    for op in ops(t):
        assert_runtime_error(op, "op after successful commit")

    a = store.begin()
    b = store.begin()
    b.put(1, "x")
    b.commit()
    a.put(1, "y")
    assert_conflict(a.commit, "write-write conflict")
    for op in ops(a):
        assert_runtime_error(op, "op after failed commit")
    store.close()


def test_values_and_extremes(tmp):
    store = lake.Store(os.path.join(tmp, "extremes.db"))
    t = store.begin()
    t.put(INT64_MIN, "min")
    t.put(INT64_MAX, "max")
    t.put(0, "")           # empty string is a real value, distinct from None
    t.commit()
    t = store.begin()
    check(t.get(INT64_MIN) == "min", "int64 min key")
    check(t.get(INT64_MAX) == "max", "int64 max key")
    check(t.get(0) == "", "empty string payload")
    check(t.get(4) is None, "missing key is None, not empty string")
    got = t.scan(INT64_MIN, INT64_MAX)  # hi exclusive: INT64_MAX cannot appear
    check(got == [(INT64_MIN, "min"), (0, "")],
          "scan over full int64 range (exclusive hi)")
    t.abort()
    store.close()


def test_reopen_and_handles(tmp):
    path = os.path.join(tmp, "reopen.db")
    store = lake.Store(path)
    t = store.begin()
    t.put(1, "one")
    t.put(-2, "minus-two")
    t.commit()
    store.close()

    store2 = lake.Store(path)   # reopen: committed data preserved
    t = store2.begin()
    check(t.get(1) == "one" and t.get(-2) == "minus-two",
          "reopen preserves committed data")
    t.abort()

    # multiple Store handles in the same process on the same file
    h1 = lake.Store(path)
    h2 = lake.Store(path)
    a = h1.begin()
    b = h2.begin()
    a.put(10, "ten")
    b.put(11, "eleven")
    a.commit()
    b.commit()
    t = store2.begin()
    check(t.get(10) == "ten" and t.get(11) == "eleven",
          "multi-handle commits visible")
    t.abort()
    store2.close()
    h1.close()
    h2.close()


# ---------------------------------------------------------------------------
# 2. deterministic randomized interleaving vs the reference model
# ---------------------------------------------------------------------------

def random_crosscheck(tmp, seed, steps=2000, nkeys=12, max_live=6):
    rng = random.Random(seed)
    db = os.path.join(tmp, "rand-%d.db" % seed)
    store = lake.Store(db)
    model = refmodel.Store()
    live = []   # list of (lake_txn, model_txn)
    seqs = []
    try:
        for _ in range(steps):
            action = rng.choice(["begin", "get", "put", "delete",
                                 "scan", "commit", "abort"])
            if action == "begin" or not live:
                if len(live) < max_live:
                    live.append((store.begin(), model.begin()))
                continue
            lt, mt = rng.choice(live)
            if action == "get":
                k = rng.randrange(-nkeys, nkeys)
                r1, r2 = lt.get(k), mt.get(k)
                check(r1 == r2, "get(%d): %r != %r" % (k, r1, r2))
            elif action == "put":
                k = rng.randrange(-nkeys, nkeys)
                v = rng.choice(["", "a", "bb", "ccc", str(rng.randrange(1000))])
                lt.put(k, v)
                mt.put(k, v)
            elif action == "delete":
                k = rng.randrange(-nkeys, nkeys)
                lt.delete(k)
                mt.delete(k)
            elif action == "scan":
                lo = rng.randrange(-nkeys, nkeys + 1)
                hi = rng.randrange(lo, nkeys + 1)
                r1, r2 = lt.scan(lo, hi), mt.scan(lo, hi)
                check(r1 == r2, "scan(%d,%d): %r != %r" % (lo, hi, r1, r2))
            else:
                pair = (lt, mt)
                live.remove(pair)
                if action == "commit":
                    e1 = e2 = None
                    try:
                        s1 = lt.commit()
                    except Exception as e:  # noqa: BLE001
                        e1 = e
                    try:
                        s2 = mt.commit()
                    except Exception as e:  # noqa: BLE001
                        e2 = e
                    if e1 is None and e2 is None:
                        check(s1 == s2, "commit number %r != model %r" % (s1, s2))
                        seqs.append(s1)
                    else:
                        check(type(e1).__name__ == "Conflict"
                              and type(e2).__name__ == "Conflict",
                              "conflict mismatch: %r vs %r" % (e1, e2))
                else:
                    lt.abort()
                    mt.abort()
        for lt, mt in live:
            lt.abort()
            mt.abort()
        check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs),
              "commit numbers strictly increasing: %r" % (seqs,))
    finally:
        store.close()

    # reopen: committed data survives and matches the model's final snapshot
    store = lake.Store(db)
    lt = store.begin()
    mt = model.begin()
    check(lt.scan(-nkeys, nkeys) == mt.scan(-nkeys, nkeys),
          "final state after reopen matches model")
    for k in range(-nkeys, nkeys):
        check(lt.get(k) == mt.get(k), "final get(%d) mismatch" % k)
    lt.abort()
    store.close()


# ---------------------------------------------------------------------------
# 3. real multi-process interleavings
# ---------------------------------------------------------------------------

class Worker:
    """A lake.Store living in a separate OS process, driven over pipes."""

    def __init__(self, db_path, name):
        self.name = name
        self.p = subprocess.Popen(
            [sys.executable, "-u", os.path.join(HERE, "lake_worker.py"), db_path],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        ready = json.loads(self.p.stdout.readline())
        check(ready.get("ready") is True, "worker %s did not start" % name)

    def cmd(self, *args):
        self.p.stdin.write(json.dumps(args) + "\n")
        self.p.stdin.flush()
        resp = json.loads(self.p.stdout.readline())
        if "error" in resp:
            raise AssertionError("worker %s error: %s" % (self.name, resp["error"]))
        return resp

    def scan(self, lo, hi):
        return [tuple(p) for p in self.cmd("scan", lo, hi)["ok"]]

    def close(self):
        try:
            self.cmd("quit")
            self.p.wait(timeout=15)
        except Exception:  # noqa: BLE001
            self.p.kill()


def test_two_process_scripted(tmp):
    db = os.path.join(tmp, "dual.db")
    a = Worker(db, "A")
    b = Worker(db, "B")
    seqs = []
    try:
        # spec example across processes: scan [10,20) vs put(15)
        a.cmd("begin")
        b.cmd("begin")
        check(a.scan(10, 20) == [], "A: empty scan across processes")
        b.cmd("put", 15, "B15")
        r = b.cmd("commit")
        check("ok" in r, "B commits")
        seqs.append(r["ok"])
        a.cmd("put", 1, "A1")
        r = a.cmd("commit")
        check(r.get("conflict") is True,
              "A must conflict across processes (scan [10,20) vs write 15)")

        # A's batch was not published; B's was
        b.cmd("begin")
        check(b.cmd("get", 1)["ok"] is None, "A's write not published")
        check(b.cmd("get", 15)["ok"] == "B15", "B's write visible")
        r = b.cmd("commit")
        seqs.append(r["ok"])

        # disjoint writes both succeed
        a.cmd("begin")
        b.cmd("begin")
        a.cmd("put", 1, "A1")
        b.cmd("put", 2, "B2")
        ra = a.cmd("commit")
        rb = b.cmd("commit")
        check("ok" in ra and "ok" in rb, "disjoint writes both commit")
        seqs += [ra["ok"], rb["ok"]]

        # point-read conflict across processes
        a.cmd("begin")
        b.cmd("begin")
        check(a.cmd("get", 2)["ok"] == "B2", "A reads B's key")
        b.cmd("put", 2, "B2v2")
        r = b.cmd("commit")
        check("ok" in r, "B commits overwrite")
        seqs.append(r["ok"])
        a.cmd("put", 3, "A3")
        check(a.cmd("commit").get("conflict") is True,
              "A conflicts on point-read key across processes")

        # snapshot isolation across processes
        a.cmd("begin")
        check(a.cmd("get", 2)["ok"] == "B2v2", "A sees committed value")
        b.cmd("begin")
        b.cmd("put", 2, "B2v3")
        r = b.cmd("commit")
        check("ok" in r, "B commits again")
        seqs.append(r["ok"])
        check(a.cmd("get", 2)["ok"] == "B2v2",
              "A still sees its snapshot across processes")
        a.cmd("abort")
    finally:
        a.close()
        b.close()

    check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs),
          "commit numbers strictly increasing across processes: %r" % (seqs,))

    # persistence: all processes gone, reopen in this process
    store = lake.Store(db)
    t = store.begin()
    check(t.get(1) == "A1" and t.get(2) == "B2v3" and t.get(15) == "B15",
          "committed data survives reopen after workers exit")
    check(t.get(3) is None, "conflicted batch not published")
    t.abort()
    store.close()


def test_two_process_random(tmp, seed=7, steps=600, nproc=3, nkeys=8):
    rng = random.Random(seed)
    db = os.path.join(tmp, "dual-random.db")
    procs = [Worker(db, "P%d" % i) for i in range(nproc)]
    model = refmodel.Store()
    live = [None] * nproc   # one open model txn per worker (worker mirrors it)
    seqs = []
    try:
        for _ in range(steps):
            i = rng.randrange(nproc)
            if live[i] is None:
                procs[i].cmd("begin")
                live[i] = model.begin()
                continue
            mt = live[i]
            action = rng.choice(["get", "get", "put", "put", "delete",
                                 "scan", "commit", "abort"])
            if action == "get":
                k = rng.randrange(-nkeys, nkeys)
                r = procs[i].cmd("get", k)["ok"]
                e = mt.get(k)
                check(r == e, "[%d] get(%d): %r != %r" % (i, k, r, e))
            elif action == "put":
                k = rng.randrange(-nkeys, nkeys)
                v = rng.choice(["", "x", "yz", str(rng.randrange(100))])
                procs[i].cmd("put", k, v)
                mt.put(k, v)
            elif action == "delete":
                k = rng.randrange(-nkeys, nkeys)
                procs[i].cmd("delete", k)
                mt.delete(k)
            elif action == "scan":
                lo = rng.randrange(-nkeys, nkeys + 1)
                hi = rng.randrange(lo, nkeys + 1)
                r = procs[i].scan(lo, hi)
                e = mt.scan(lo, hi)
                check(r == e, "[%d] scan(%d,%d): %r != %r" % (i, lo, hi, r, e))
            elif action == "commit":
                r = procs[i].cmd("commit")
                try:
                    e = mt.commit()
                    ce = None
                except Exception as ex:  # noqa: BLE001
                    e, ce = None, ex
                if ce is None:
                    check("ok" in r and r["ok"] == e,
                          "[%d] commit: %r != model seq %r" % (i, r, e))
                    seqs.append(e)
                else:
                    check(r.get("conflict") is True,
                          "[%d] commit: expected conflict, got %r" % (i, r))
                live[i] = None
            else:  # abort
                procs[i].cmd("abort")
                mt.abort()
                live[i] = None
        for i in range(nproc):
            if live[i] is not None:
                procs[i].cmd("abort")
                live[i].abort()
                live[i] = None
    finally:
        for p in procs:
            p.close()

    check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs),
          "commit numbers strictly increasing across processes: %r" % (seqs,))

    # persistence + final state matches the model
    store = lake.Store(db)
    lt = store.begin()
    mt = model.begin()
    check(lt.scan(-nkeys, nkeys) == mt.scan(-nkeys, nkeys),
          "final state after multi-process run matches model")
    lt.abort()
    store.close()


# ---------------------------------------------------------------------------

def main():
    tmp = tempfile.mkdtemp(prefix="lake-verify-")
    try:
        test_basics(tmp)
        print("ok  basics: get/put/delete/scan/commit/abort")
        test_snapshot(tmp)
        print("ok  snapshot isolation")
        test_conflicts(tmp)
        print("ok  conflict rules (incl. spec examples, boundaries, "
              "same-value write, delete-of-missing)")
        test_invalidation(tmp)
        print("ok  handle invalidation after commit/conflict/abort")
        test_values_and_extremes(tmp)
        print("ok  values & int64 extremes")
        test_reopen_and_handles(tmp)
        print("ok  reopen persistence & same-process multi-handle")
        for seed in (1, 2, 3):
            random_crosscheck(tmp, seed)
            print("ok  randomized interleaving vs reference model (seed=%d)"
                  % seed)
        test_two_process_scripted(tmp)
        print("ok  dual-process scripted interleaving")
        test_two_process_random(tmp)
        print("ok  multi-process randomized interleaving vs model")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    print("VERIFY OK")


if __name__ == "__main__":
    main()
