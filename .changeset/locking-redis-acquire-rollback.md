---
"@medusajs/locking-redis": patch
---

fix(locking-redis): acquire multi-key locks in a stable order, track per-call claims with an awaited two-phase commit so partial acquisitions roll back atomically and no call can resolve after losing its lease, keep release owner-final, make each releaseAll deletion an atomic compare-and-delete that surfaces command errors, and fix same-owner reentrancy under awaitQueue
