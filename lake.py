"""lake -- snapshot-isolated transactional key/value batches on SQLite.

Usage::

    import lake
    store = lake.Store(path)
    txn = store.begin()
    txn.put(1, "hello")
    txn.commit()            # -> global, strictly increasing commit number

Semantics
---------
* Keys and scan bounds are signed 64-bit integers, payloads are strings.
* ``begin()`` captures a snapshot (the current commit sequence number).
  Reads see that snapshot overlaid with the transaction's own puts/deletes;
  a missing (or deleted) key reads as ``None``.  ``scan(lo, hi)`` returns the
  sorted ``(k, v)`` list for ``lo <= k < hi``.
* ``commit()`` validates and publishes atomically: if any other transaction
  that committed after this one began wrote a key this transaction wrote,
  point-read, or scanned over, ``commit()`` raises :class:`lake.Conflict`
  and publishes nothing.  Otherwise the whole batch is published and the new
  commit number is returned.  Same-value writes and deletes of missing keys
  still count as writes.  With no such intersection, commit always succeeds;
  validation only consults this transaction's read/write keys and scan ranges.
* ``abort()`` discards the batch.  After ``commit()`` (success or conflict)
  or ``abort()``, the handle is dead: every further operation raises
  ``RuntimeError``.

Implementation notes
--------------------
* Multi-version storage: every committed write appends a row to
  ``versions(k, seq, v)`` (``v IS NULL`` is a delete tombstone).  Nothing is
  ever updated or deleted, so any snapshot stays readable without copying
  the table.  No version garbage collection is performed.
* No SQLite transaction is held across calls: reads run as single autocommit
  statements; ``commit()`` wraps validate+publish in one ``BEGIN IMMEDIATE``
  transaction, which also serializes committers across processes (WAL mode
  plus a busy timeout makes concurrent readers/writers coexist).
"""

import sqlite3

__all__ = ["Store", "Conflict", "Transaction"]

_BUSY_TIMEOUT_MS = 30000

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS commit_log ("
    "    seq INTEGER PRIMARY KEY AUTOINCREMENT"
    ")",
    "CREATE TABLE IF NOT EXISTS versions ("
    "    k   INTEGER NOT NULL,"
    "    seq INTEGER NOT NULL,"
    "    v   TEXT,"
    "    PRIMARY KEY (k, seq)"
    ")",
)


class Conflict(Exception):
    """commit() found an intersecting write committed after this transaction began."""


class Store:
    """A lake repository backed by a SQLite database file at ``path``."""

    def __init__(self, path):
        self._path = path
        self._db = sqlite3.connect(path, isolation_level=None,
                                   timeout=_BUSY_TIMEOUT_MS / 1000.0)
        self._db.execute("PRAGMA busy_timeout = %d" % _BUSY_TIMEOUT_MS)
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._init_schema()

    def _init_schema(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            for stmt in _SCHEMA:
                self._db.execute(stmt)
            self._db.execute("COMMIT")
        except BaseException:
            _rollback_quietly(self._db)
            raise

    def begin(self):
        """Open a new transaction at the current snapshot."""
        snap = self._db.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM commit_log").fetchone()[0]
        return Transaction(self._db, snap)

    def close(self):
        self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _rollback_quietly(db):
    try:
        db.execute("ROLLBACK")
    except sqlite3.Error:
        pass


class Transaction:
    """One snapshot-isolated batch.  Obtain via :meth:`Store.begin`."""

    def __init__(self, db, snap):
        self._db = db
        self._snap = snap
        self._writes = {}        # k -> value, or None for delete
        self._read_keys = set()  # point-read keys
        self._scan_ranges = []   # [lo, hi) ranges
        self._closed = False

    # -- internal ---------------------------------------------------------

    def _require_open(self):
        if self._closed:
            raise RuntimeError("transaction handle is no longer valid")

    # -- reads ------------------------------------------------------------

    def get(self, k):
        """Value of key ``k`` at the snapshot overlaid with own writes, or None."""
        self._require_open()
        k = int(k)
        self._read_keys.add(k)
        if k in self._writes:
            return self._writes[k]
        row = self._db.execute(
            "SELECT v FROM versions WHERE k = ? AND seq <= ? "
            "ORDER BY seq DESC LIMIT 1",
            (k, self._snap)).fetchone()
        return row[0] if row is not None else None

    def scan(self, lo, hi):
        """Sorted ``(k, v)`` list for ``lo <= k < hi`` (snapshot + own writes)."""
        self._require_open()
        lo = int(lo)
        hi = int(hi)
        self._scan_ranges.append((lo, hi))
        rows = self._db.execute(
            "SELECT versions.k, versions.v FROM versions "
            "JOIN ("
            "    SELECT k AS lk, MAX(seq) AS ls FROM versions "
            "    WHERE k >= ? AND k < ? AND seq <= ? GROUP BY k"
            ") AS latest "
            "ON versions.k = latest.lk AND versions.seq = latest.ls "
            "WHERE versions.v IS NOT NULL",
            (lo, hi, self._snap)).fetchall()
        merged = dict(rows)
        for k, v in self._writes.items():
            if lo <= k < hi:
                if v is None:
                    merged.pop(k, None)
                else:
                    merged[k] = v
        return sorted(merged.items())

    # -- writes -----------------------------------------------------------

    def put(self, k, v):
        """Stage a write.  Writing the existing value still counts as a write."""
        self._require_open()
        self._writes[int(k)] = v

    def delete(self, k):
        """Stage a delete.  Deleting a missing key still counts as a write."""
        self._require_open()
        self._writes[int(k)] = None

    # -- lifecycle ----------------------------------------------------------

    def commit(self):
        """Validate and publish atomically; return the new commit number.

        Raises :class:`Conflict` (publishing nothing) if any transaction that
        committed after this one began wrote a key this transaction wrote,
        point-read, or scanned over.
        """
        self._require_open()
        self._closed = True
        db = self._db
        snap = self._snap
        db.execute("BEGIN IMMEDIATE")
        try:
            for k in self._read_keys | set(self._writes):
                if db.execute(
                        "SELECT 1 FROM versions WHERE k = ? AND seq > ? LIMIT 1",
                        (k, snap)).fetchone():
                    raise Conflict(
                        "key %r was written by a concurrent commit" % k)
            for lo, hi in self._scan_ranges:
                if db.execute(
                        "SELECT 1 FROM versions "
                        "WHERE k >= ? AND k < ? AND seq > ? LIMIT 1",
                        (lo, hi, snap)).fetchone():
                    raise Conflict(
                        "scan range [%r, %r) intersects a concurrent commit"
                        % (lo, hi))
            seq = db.execute("INSERT INTO commit_log DEFAULT VALUES").lastrowid
            db.executemany(
                "INSERT INTO versions (k, seq, v) VALUES (?, ?, ?)",
                [(k, seq, v) for k, v in self._writes.items()])
            db.execute("COMMIT")
        except BaseException:
            _rollback_quietly(db)
            raise
        return seq

    def abort(self):
        """Discard the batch; nothing is published."""
        self._require_open()
        self._closed = True
