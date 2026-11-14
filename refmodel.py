"""Independent in-memory reference implementation of lake's snapshot semantics.

This is the "snapshot reference": a minimal, obviously-correct model used by
verify.py to cross-check the real SQLite-backed implementation.  Committed
batches are appended to a timeline; a transaction's snapshot is the timeline
length at begin().  Conflict rules mirror lake exactly:

  a commit fails iff some batch committed after this transaction's snapshot
  contains a key this transaction wrote, point-read, or scanned over
  (scan ranges are [lo, hi)); same-value writes and deletes of missing keys
  count as writes.
"""


class Conflict(Exception):
    pass


class Store:
    def __init__(self):
        self._timeline = []  # committed write-sets; commit number = index + 1

    def begin(self):
        return Transaction(self, len(self._timeline))


class Transaction:
    def __init__(self, store, snap):
        self._store = store
        self._snap = snap
        self._writes = {}
        self._reads = set()
        self._scans = []
        self._closed = False

    def _require_open(self):
        if self._closed:
            raise RuntimeError("transaction handle is no longer valid")

    def get(self, k):
        self._require_open()
        self._reads.add(k)
        if k in self._writes:
            return self._writes[k]
        for batch in reversed(self._store._timeline[:self._snap]):
            if k in batch:
                return batch[k]
        return None

    def scan(self, lo, hi):
        self._require_open()
        self._scans.append((lo, hi))
        latest = {}
        for batch in self._store._timeline[:self._snap]:
            for k, v in batch.items():
                if lo <= k < hi:
                    latest[k] = v
        merged = {k: v for k, v in latest.items() if v is not None}
        for k, v in self._writes.items():
            if lo <= k < hi:
                if v is None:
                    merged.pop(k, None)
                else:
                    merged[k] = v
        return sorted(merged.items())

    def put(self, k, v):
        self._require_open()
        self._writes[k] = v

    def delete(self, k):
        self._require_open()
        self._writes[k] = None

    def commit(self):
        self._require_open()
        self._closed = True
        for batch in self._store._timeline[self._snap:]:
            for k in batch:
                if (k in self._writes or k in self._reads
                        or any(lo <= k < hi for lo, hi in self._scans)):
                    raise Conflict("intersecting key %r" % (k,))
        self._store._timeline.append(dict(self._writes))
        return len(self._store._timeline)

    def abort(self):
        self._require_open()
        self._closed = True
