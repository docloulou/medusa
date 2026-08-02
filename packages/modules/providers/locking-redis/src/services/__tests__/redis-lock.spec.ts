import { setTimeout } from "node:timers/promises"
import { RedisLockingProvider } from "../redis-lock"

jest.mock("node:timers/promises", () => ({
  setTimeout: jest.fn().mockResolvedValue(undefined),
}))

describe("RedisLockingProvider Jitter", () => {
  let provider: RedisLockingProvider
  const redisClientMock = {
    defineCommand: jest.fn(),
    acquireLock: jest.fn(),
    commitLock: jest.fn().mockResolvedValue(1),
    finalizeLock: jest.fn().mockResolvedValue(1),
    rollbackLock: jest.fn().mockResolvedValue(1),
  }

  beforeEach(() => {
    provider = new RedisLockingProvider(
      {
        redisClient: redisClientMock as any,
        prefix: "test:",
      },
      {
        defaultRetryInterval: 100,
        backoffFactor: 2,
      } as any
    )
    jest.clearAllMocks()
  })

  it("should apply jitter between 50% and 100% of the retryDelay", async () => {
    // Mock acquireLock to fail once then succeed
    redisClientMock.acquireLock
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(1)

    await provider.acquire("test-key", { awaitQueue: true })

    // The first retryDelay should be the defaultRetryInterval (100)
    const expectedBaseDelay = 100
    
    expect(setTimeout).toHaveBeenCalledTimes(1)
    const actualDelay = (setTimeout as jest.Mock).mock.calls[0][0]
    
    expect(actualDelay).toBeGreaterThanOrEqual(expectedBaseDelay * 0.5)
    expect(actualDelay).toBeLessThanOrEqual(expectedBaseDelay)
  })

  it("should apply jitter to subsequent exponential backoff steps", async () => {
    // Mock acquireLock to fail twice then succeed
    redisClientMock.acquireLock
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(0)
      .mockResolvedValueOnce(1)

    await provider.acquire("test-key", { awaitQueue: true })

    expect(setTimeout).toHaveBeenCalledTimes(2)
    
    // First delay (base 100)
    const firstDelay = (setTimeout as jest.Mock).mock.calls[0][0]
    expect(firstDelay).toBeGreaterThanOrEqual(100 * 0.5)
    expect(firstDelay).toBeLessThanOrEqual(100)

    // Second delay (base 200, due to backoffFactor 2)
    const secondDelay = (setTimeout as jest.Mock).mock.calls[1][0]
    expect(secondDelay).toBeGreaterThanOrEqual(200 * 0.5)
    expect(secondDelay).toBeLessThanOrEqual(200)
  })
})

describe("RedisLockingProvider acquire ordering and rollback", () => {
  let provider: RedisLockingProvider
  const redisClientMock = {
    defineCommand: jest.fn(),
    acquireLock: jest.fn(),
    releaseLock: jest.fn(),
    rollbackLock: jest.fn(),
    commitLock: jest.fn().mockResolvedValue(1),
    finalizeLock: jest.fn().mockResolvedValue(1),
    scan: jest.fn(),
    pipeline: jest.fn(),
  }

  beforeEach(() => {
    provider = new RedisLockingProvider(
      {
        redisClient: redisClientMock as any,
        prefix: "test:",
      },
      {
        defaultRetryInterval: 10,
        backoffFactor: 2,
      } as any
    )
    jest.clearAllMocks()
    // clearAllMocks keeps mockImplementation overrides; pin the defaults so
    // a per-test commit failure cannot leak into its neighbors.
    redisClientMock.commitLock.mockResolvedValue(1)
    redisClientMock.finalizeLock.mockResolvedValue(1)
  })

  it("should deduplicate and sort keys and acquire them sequentially", async () => {
    let resolveFirst!: (result: number) => void
    redisClientMock.acquireLock
      .mockImplementationOnce(
        () => new Promise<number>((resolve) => (resolveFirst = resolve))
      )
      .mockResolvedValue(1)

    const acquiring = provider.acquire(["b", "a", "b"], { ownerId: "owner_1" })

    // Until the first sorted key resolves, nothing else may be in flight.
    expect(redisClientMock.acquireLock.mock.calls).toEqual([
      [
        "test:a",
        "medusa_lock_token:{test:a}:test:a",
        "owner_1",
        0,
        expect.any(String),
      ],
    ])

    resolveFirst(1)
    await acquiring

    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.acquireLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", 0, token],
      ["test:b", "medusa_lock_token:{test:b}:test:b", "owner_1", 0, token],
    ])
  })

  it("should commit claims after a successful call", async () => {
    redisClientMock.acquireLock.mockResolvedValue(1)
    redisClientMock.commitLock.mockResolvedValue(1)

    await provider.acquire(["b", "a"], { ownerId: "owner_1" })

    // Phase one marks each claim committed (awaited); phase two folds the
    // marks into the durable 'held' marker, so repeated refreshes cannot
    // grow the sidecar hash.
    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.commitLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", token],
      ["test:b", "medusa_lock_token:{test:b}:test:b", "owner_1", token],
    ])
    expect(redisClientMock.finalizeLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", token],
      ["test:b", "medusa_lock_token:{test:b}:test:b", "owner_1", token],
    ])
    expect(redisClientMock.rollbackLock).not.toHaveBeenCalled()
  })

  it("should fail the call and roll back when a commit mark is rejected", async () => {
    redisClientMock.acquireLock.mockResolvedValue(1)
    redisClientMock.commitLock.mockImplementation((key: string) =>
      key === "test:b"
        ? Promise.reject(new Error("connection lost"))
        : Promise.resolve(1)
    )
    redisClientMock.rollbackLock.mockResolvedValue(1)

    await expect(
      provider.acquire(["b", "a"], { ownerId: "owner_1" })
    ).rejects.toThrow('Failed to acquire lock for keys "a", "b"')

    // The marks stay token-identified until every one has landed, so the
    // rollback can withdraw both the marked and the unmarked claim.
    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.rollbackLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", token],
      ["test:b", "medusa_lock_token:{test:b}:test:b", "owner_1", token],
    ])
    expect(redisClientMock.finalizeLock).not.toHaveBeenCalled()
  })

  it("should fail the call when a commit finds the lease gone", async () => {
    redisClientMock.acquireLock.mockResolvedValue(1)
    // The lease for "a" expired (or was released and re-acquired) while the
    // call was still acquiring "b": the commit's verification reports 0.
    redisClientMock.commitLock.mockImplementation((key: string) =>
      Promise.resolve(key === "test:a" ? 0 : 1)
    )
    redisClientMock.rollbackLock.mockResolvedValue(1)

    await expect(
      provider.acquire(["b", "a"], { ownerId: "owner_1" })
    ).rejects.toThrow('Failed to acquire lock for keys "a", "b"')
    expect(redisClientMock.finalizeLock).not.toHaveBeenCalled()
  })

  it("should roll back the keys a failed call took, and only those", async () => {
    redisClientMock.acquireLock.mockImplementation((key: string) =>
      Promise.resolve(key === "test:b" ? 0 : 1)
    )
    redisClientMock.rollbackLock.mockResolvedValue(1)

    await expect(
      provider.acquire(["c", "a", "b"], { ownerId: "owner_1" })
    ).rejects.toThrow('Failed to acquire lock for key "b"')

    // Sorted order: "a" succeeds, "b" fails, "c" is never attempted. The
    // rollback targets the exact acquisition: owner and per-call token.
    expect(redisClientMock.acquireLock).toHaveBeenCalledTimes(2)
    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.rollbackLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", token],
    ])
    expect(redisClientMock.releaseLock).not.toHaveBeenCalled()
  })

  it("should withdraw claims from re-entered keys without deleting them", async () => {
    const results: Record<string, number> = {
      "test:a": 2,
      "test:b": 1,
      "test:c": 0,
    }
    redisClientMock.acquireLock.mockImplementation((key: string) =>
      Promise.resolve(results[key])
    )
    redisClientMock.rollbackLock.mockResolvedValue(1)

    await expect(
      provider.acquire(["a", "b", "c"], { ownerId: "owner_1" })
    ).rejects.toThrow('Failed to acquire lock for key "c"')

    // "a" was re-entered (code 2): this call registered a claim on it, so
    // the rollback must withdraw that claim too. Whether a withdrawal
    // deletes the lease is decided inside the script - a pre-held key still
    // carries its creator's claim and survives (pinned by the integration
    // suite against real Redis).
    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.rollbackLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "owner_1", token],
      ["test:b", "medusa_lock_token:{test:b}:test:b", "owner_1", token],
    ])
  })

  it("should roll back anonymous acquisitions through their token", async () => {
    const results: Record<string, number> = {
      "test:a": 1,
      "test:b": 0,
    }
    redisClientMock.acquireLock.mockImplementation((key: string) =>
      Promise.resolve(results[key])
    )
    redisClientMock.rollbackLock.mockResolvedValue(1)

    await expect(provider.acquire(["a", "b"])).rejects.toThrow(
      'Failed to acquire lock for key "b"'
    )

    // Two anonymous holders are indistinguishable by owner, but the token
    // pins the exact acquisition, so the rollback is safe for "*" too.
    const token = redisClientMock.acquireLock.mock.calls[0][4]
    expect(redisClientMock.rollbackLock.mock.calls).toEqual([
      ["test:a", "medusa_lock_token:{test:a}:test:a", "*", token],
    ])
  })

  it("should reject a prefix that overlaps the token namespace", () => {
    // With such a prefix a lock's logical name could spell out another
    // lock's sidecar key; the two namespaces must stay prefix-disjoint.
    for (const prefix of ["", "m", "medusa_lock_token:", "medusa_lock_token:x:"]) {
      expect(
        () =>
          new RedisLockingProvider(
            { redisClient: redisClientMock as any, prefix },
            {} as any
          )
      ).toThrow(/overlaps/)
    }
  })

  it("should reject a client configured with an ioredis keyPrefix", () => {
    // ioredis rewrites declared script keys but not SCAN patterns/results,
    // which would break releaseAll and lock/sidecar slot alignment.
    for (const options of [
      { keyPrefix: "app:" },
      { redisOptions: { keyPrefix: "app:" } },
    ]) {
      expect(
        () =>
          new RedisLockingProvider(
            {
              redisClient: { ...redisClientMock, options } as any,
              prefix: "test:",
            },
            {} as any
          )
      ).toThrow(/keyPrefix/)
    }
  })

  it("should treat string script replies as numbers", async () => {
    // ioredis `stringNumbers: true` turns Lua integer replies into strings.
    redisClientMock.acquireLock.mockResolvedValue("1" as any)
    redisClientMock.commitLock.mockResolvedValue("1" as any)
    redisClientMock.releaseLock.mockResolvedValue("1" as any)

    await provider.acquire("s", { ownerId: "owner_1" })

    expect(redisClientMock.acquireLock).toHaveBeenCalledTimes(1)
    expect(redisClientMock.rollbackLock).not.toHaveBeenCalled()

    // release must report true, not compare "1" === 1.
    await expect(provider.release("s", { ownerId: "owner_1" })).resolves.toBe(
      true
    )
  })

  it("should keep the lock key's hash tag in the sidecar key name", async () => {
    redisClientMock.acquireLock.mockResolvedValue(1)

    await provider.acquire("{tenant_1}:cart", { ownerId: "owner_1" })

    // The lock key already carries a hash tag, so the sidecar reuses it as
    // its first brace pair and hashes to the same cluster slot.
    expect(redisClientMock.acquireLock.mock.calls).toEqual([
      [
        "test:{tenant_1}:cart",
        "medusa_lock_token:{tenant_1}:test:{tenant_1}:cart",
        "owner_1",
        0,
        expect.any(String),
      ],
    ])
  })

  it("should keep sidecar keys out of the primary lock keyspace", async () => {
    redisClientMock.acquireLock.mockResolvedValue(1)

    // Even a lock whose logical name spells out another lock's sidecar name
    // stays inside the primary prefix and cannot touch that sidecar.
    await provider.acquire("medusa_lock_token:{test:a}:test:a", {
      ownerId: "owner_1",
    })

    const [keyName, tokenKeyName] = redisClientMock.acquireLock.mock.calls[0]
    expect(keyName).toBe("test:medusa_lock_token:{test:a}:test:a")
    expect(tokenKeyName).toBe(
      "medusa_lock_token:{test:a}:test:medusa_lock_token:{test:a}:test:a"
    )
  })

  it("should keep every sidecar in the same cluster slot as its lock key", () => {
    // Redis cluster slot: CRC16-XMODEM of the effective hash content - the
    // first non-empty {tag} when present, otherwise the whole key name.
    const crc16 = (str: string): number => {
      let crc = 0
      for (let i = 0; i < str.length; i++) {
        crc ^= str.charCodeAt(i) << 8
        for (let bit = 0; bit < 8; bit++) {
          crc =
            crc & 0x8000
              ? ((crc << 1) ^ 0x1021) & 0xffff
              : (crc << 1) & 0xffff
        }
      }
      return crc
    }
    const hashSlot = (key: string): number => {
      const open = key.indexOf("{")
      if (open >= 0) {
        const close = key.indexOf("}", open + 1)
        if (close > open + 1) {
          return crc16(key.slice(open + 1, close)) % 16384
        }
      }
      return crc16(key) % 16384
    }

    const keyNames = [
      "test:plain",
      "test:{tenant_1}:cart",
      // '{' without a closing '}': the whole name is the hash content.
      "test:a{open",
      // stray '}' before a valid tag: the tag still wins.
      "test:}a{b}c",
      "test:medusa_lock_token:{x}:y",
    ]
    for (const keyName of keyNames) {
      const tokenKeyName = (provider as any).getTokenKeyName(keyName)
      expect(hashSlot(tokenKeyName)).toBe(hashSlot(keyName))
    }
  })

  it("should reject lock keys whose sidecar cannot share their hash slot", async () => {
    // A '}' without a valid hash tag makes the whole name the hash content,
    // which no brace-wrapped sidecar can reproduce.
    await expect(
      provider.acquire("a}b", { ownerId: "owner_1" })
    ).rejects.toThrow(/hash slot/)
    expect(redisClientMock.acquireLock).not.toHaveBeenCalled()
  })

  it("should release valid keys but fail loudly on unreleasable scanned keys", async () => {
    redisClientMock.scan.mockResolvedValueOnce(["0", ["test:a}b", "test:ok"]])
    const pipelineMock = {
      releaseLock: jest.fn().mockReturnThis(),
      exec: jest.fn().mockResolvedValue([]),
    }
    redisClientMock.pipeline.mockReturnValue(pipelineMock)

    // Every releasable lock is released, and the call still surfaces the
    // keys it could not touch instead of reporting a clean sweep.
    await expect(
      provider.releaseAll({ ownerId: "owner_1" })
    ).rejects.toThrow(/could not release 1 key/)

    expect(pipelineMock.releaseLock.mock.calls).toEqual([
      ["test:ok", "medusa_lock_token:{test:ok}:test:ok", "owner_1"],
    ])
    expect(pipelineMock.exec).toHaveBeenCalledTimes(1)
  })

  it("should re-enter an already-held lock immediately under awaitQueue", async () => {
    redisClientMock.acquireLock
      .mockResolvedValueOnce(2)
      .mockRejectedValue(new Error("unexpected retry"))

    await provider.acquire("k", { ownerId: "owner_1", awaitQueue: true })

    expect(redisClientMock.acquireLock).toHaveBeenCalledTimes(1)
    expect(setTimeout).not.toHaveBeenCalled()
  })

  it("should surface pipeline command failures from releaseAll", async () => {
    // pipeline.exec resolves [error, result] tuples instead of rejecting;
    // an ACL or script failure must not report a clean sweep.
    redisClientMock.scan.mockResolvedValueOnce(["0", ["test:x"]])
    const pipelineMock = {
      releaseLock: jest.fn().mockReturnThis(),
      exec: jest
        .fn()
        .mockResolvedValue([[new Error("NOPERM this user has no permissions"), null]]),
    }
    redisClientMock.pipeline.mockReturnValue(pipelineMock)

    await expect(provider.releaseAll({ ownerId: "owner_1" })).rejects.toThrow(
      /NOPERM/
    )
  })

  it("should release scanned keys through a single atomic pipeline", async () => {
    redisClientMock.scan.mockResolvedValueOnce(["0", ["test:x", "test:y"]])

    const pipelineMock = {
      releaseLock: jest.fn().mockReturnThis(),
      exec: jest.fn().mockResolvedValue([]),
    }
    redisClientMock.pipeline.mockReturnValue(pipelineMock)

    await provider.releaseAll({ ownerId: "owner_1" })

    expect(redisClientMock.scan).toHaveBeenCalledWith(
      "0",
      "MATCH",
      "test:*",
      "COUNT",
      100
    )
    expect(pipelineMock.releaseLock.mock.calls).toEqual([
      ["test:x", "medusa_lock_token:{test:x}:test:x", "owner_1"],
      ["test:y", "medusa_lock_token:{test:y}:test:y", "owner_1"],
    ])
    expect(pipelineMock.exec).toHaveBeenCalledTimes(1)
  })
})
