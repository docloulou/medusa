import { ILockingModule } from "@medusajs/framework/types"
import { Modules, promiseAll } from "@medusajs/framework/utils"
import { moduleIntegrationTestRunner } from "@medusajs/test-utils"
import { setTimeout } from "node:timers/promises"

jest.setTimeout(5000)

const providerId = "locking-redis"
moduleIntegrationTestRunner<ILockingModule>({
  moduleName: Modules.LOCKING,
  moduleOptions: {
    providers: [
      {
        id: providerId,
        resolve: require.resolve("../../src"),
        is_default: true,
        options: {
          redisUrl: process.env.REDIS_URL ?? "redis://localhost:6379",
        },
      },
    ],
  },
  testSuite: ({ service }) => {
    describe("Locking Module Service", () => {
      let stock = 5
      function replenishStock() {
        stock = 5
      }
      function hasStock() {
        return stock > 0
      }
      async function reduceStock() {
        await setTimeout(10)
        stock--
      }
      async function buy() {
        if (hasStock()) {
          await reduceStock()
          return true
        }
        return false
      }

      beforeEach(async () => {
        await service.releaseAll()
      })

      it("should execute functions respecting the key locked", async () => {
        // 10 parallel calls to buy should oversell the stock
        const prom: any[] = []
        for (let i = 0; i < 10; i++) {
          prom.push(buy())
        }
        await Promise.all(prom)
        expect(stock).toBe(-5)

        replenishStock()

        // 10 parallel calls to buy with lock should not oversell the stock
        const promWLock: any[] = []
        for (let i = 0; i < 10; i++) {
          promWLock.push(service.execute("item_1", buy))
        }
        await Promise.all(promWLock)

        expect(stock).toBe(0)
      })

      it("should acquire lock and release it", async () => {
        await service.acquire("key_name", {
          ownerId: "user_id_123",
        })

        const userReleased = await service.release("key_name", {
          ownerId: "user_id_456",
        })
        const anotherUserLock = service.acquire("key_name", {
          ownerId: "user_id_456",
        })

        expect(userReleased).toBe(false)
        await expect(anotherUserLock).rejects.toThrow(
          `Failed to acquire lock for key "key_name"`
        )

        const releasing = await service.release("key_name", {
          ownerId: "user_id_123",
        })

        expect(releasing).toBe(true)
      })

      it("should acquire lock and release it during parallel calls", async () => {
        const keyToLock = "mySpecialKey"
        const user_1 = {
          ownerId: "user_id_456",
        }
        const user_2 = {
          ownerId: "user_id_000",
        }

        await expect(
          service.acquire(keyToLock, user_1)
        ).resolves.toBeUndefined()

        await expect(
          service.acquire(keyToLock, user_1)
        ).resolves.toBeUndefined()

        await expect(service.acquire(keyToLock, user_2)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        await expect(service.acquire(keyToLock, user_2)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        await service.acquire(keyToLock, user_1)

        const releaseNotLocked = await service.release(keyToLock, {
          ownerId: "user_id_000",
        })
        expect(releaseNotLocked).toBe(false)

        const release = await service.release(keyToLock, user_1)
        expect(release).toBe(true)
      })

      it("should fail to acquire the same key when no owner is provided", async () => {
        const keyToLock = "mySpecialKey"

        const user_2 = {
          ownerId: "user_id_000",
        }

        await expect(service.acquire(keyToLock)).resolves.toBeUndefined()

        await expect(service.acquire(keyToLock)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        await expect(service.acquire(keyToLock)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        await expect(service.acquire(keyToLock, user_2)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        await expect(service.acquire(keyToLock, user_2)).rejects.toThrow(
          `Failed to acquire lock for key "${keyToLock}"`
        )

        const releaseNotLocked = await service.release(keyToLock, {
          ownerId: "user_id_000",
        })
        expect(releaseNotLocked).toBe(false)

        const release = await service.release(keyToLock)
        expect(release).toBe(true)
      })

      it("should re-enter a lock held by the same owner when awaitQueue is set", async () => {
        await service.acquire("reentrant_key", {
          ownerId: "owner_reentry",
          expire: 10,
        })

        await expect(
          service.acquire("reentrant_key", {
            ownerId: "owner_reentry",
            expire: 10,
            awaitQueue: true,
          })
        ).resolves.toBeUndefined()

        expect(
          await service.release("reentrant_key", { ownerId: "owner_reentry" })
        ).toBe(true)
      })

      it("should roll back the keys a failed multi-key acquire took", async () => {
        await service.acquire("rb_c", { ownerId: "owner_blocker", expire: 10 })

        await expect(
          service.acquire(["rb_c", "rb_a"], {
            ownerId: "owner_caller",
            expire: 10,
          })
        ).rejects.toThrow(`Failed to acquire lock for key "rb_c"`)

        await expect(
          service.acquire("rb_a", { ownerId: "owner_other", expire: 10 })
        ).resolves.toBeUndefined()

        expect(await service.release("rb_a", { ownerId: "owner_other" })).toBe(
          true
        )
        expect(
          await service.release("rb_c", { ownerId: "owner_blocker" })
        ).toBe(true)
      })

      it("should keep keys the owner already held when a later key fails", async () => {
        await service.acquire("pre_a", { ownerId: "owner_caller", expire: 10 })
        await service.acquire("pre_z", { ownerId: "owner_blocker", expire: 10 })

        await expect(
          service.acquire(["pre_z", "pre_a", "pre_m"], {
            ownerId: "owner_caller",
            expire: 10,
          })
        ).rejects.toThrow(`Failed to acquire lock for key "pre_z"`)

        await expect(
          service.acquire("pre_m", { ownerId: "owner_probe", expire: 10 })
        ).resolves.toBeUndefined()

        expect(
          await service.release("pre_a", { ownerId: "owner_caller" })
        ).toBe(true)

        expect(await service.release("pre_m", { ownerId: "owner_probe" })).toBe(
          true
        )
        expect(
          await service.release("pre_z", { ownerId: "owner_blocker" })
        ).toBe(true)
      })

      it("should scope releaseAll to the given owner", async () => {
        await service.acquire("ra_mine", { ownerId: "owner_a", expire: 10 })
        await service.acquire("ra_theirs", { ownerId: "owner_b", expire: 10 })

        await service.releaseAll({ ownerId: "owner_a" })

        await expect(
          service.acquire("ra_mine", { ownerId: "owner_c", expire: 10 })
        ).resolves.toBeUndefined()

        await expect(
          service.acquire("ra_theirs", { ownerId: "owner_c", expire: 10 })
        ).rejects.toThrow(`Failed to acquire lock for key "ra_theirs"`)

        expect(await service.release("ra_mine", { ownerId: "owner_c" })).toBe(
          true
        )
        expect(
          await service.release("ra_theirs", { ownerId: "owner_b" })
        ).toBe(true)
      })

      it("should roll back anonymous acquisitions on failure", async () => {
        await service.acquire("anon_b", {
          ownerId: "owner_blocker",
          expire: 10,
        })

        await expect(
          service.acquire(["anon_b", "anon_a"], { expire: 10 })
        ).rejects.toThrow(`Failed to acquire lock for key "anon_b"`)

        // "anon_a" was rolled back: a named owner can take it immediately.
        await expect(
          service.acquire("anon_a", { ownerId: "owner_probe", expire: 10 })
        ).resolves.toBeUndefined()

        expect(
          await service.release("anon_a", { ownerId: "owner_probe" })
        ).toBe(true)
        expect(
          await service.release("anon_b", { ownerId: "owner_blocker" })
        ).toBe(true)
      })
    })

    it("should release lock in case of failure", async () => {
      const fn_1 = jest.fn(async () => {
        throw new Error("Error")
      })
      const fn_2 = jest.fn(async () => {})

      await service.execute("lock_key", fn_1).catch(() => {})
      await service.execute("lock_key", fn_2).catch(() => {})

      expect(fn_1).toHaveBeenCalledTimes(1)
      expect(fn_2).toHaveBeenCalledTimes(1)
    })

    it("should release lock in case of timeout failure", async () => {
      const fn_1 = jest.fn(async () => {
        await setTimeout(1010)
        return "fn_1"
      })

      const fn_2 = jest.fn(async () => {
        return "fn_2"
      })

      const fn_3 = jest.fn(async () => {
        return "fn_3"
      })

      const ops = [
        service
          .execute("lock_key", fn_1, {
            timeout: 1,
          })
          .catch((e) => e),

        service
          .execute("lock_key", fn_2, {
            timeout: 1,
          })
          .catch((e) => e),

        service
          .execute("lock_key", fn_3, {
            timeout: 5,
          })
          .catch((e) => e),
      ]

      const res = await promiseAll(ops)

      expect(res).toEqual(["fn_1", Error("Timed-out acquiring lock."), "fn_3"])

      expect(fn_1).toHaveBeenCalledTimes(1)
      expect(fn_2).toHaveBeenCalledTimes(0)
      expect(fn_3).toHaveBeenCalledTimes(1)
    })
  },
})

describe("RedisLockingProvider rollback vs same-owner re-entry race", () => {
  const { RedisLockingProvider } = require("../../src/services/redis-lock")
  const Redis = require("ioredis")

  const prefix = "race_lock:"
  let client: any
  let provider: any

  beforeAll(() => {
    client = new Redis(process.env.REDIS_URL ?? "redis://localhost:6379")
    provider = new RedisLockingProvider(
      { redisClient: client, prefix },
      {} as any
    )
  })

  afterAll(async () => {
    const keys = await client.keys(`${prefix}*`)
    const tokenKeys = await client.keys(`medusa_lock_token:*${prefix}*`)
    const all = [...keys, ...tokenKeys]
    if (all.length) {
      await client.del(...all)
    }
    client.disconnect()
  })

  // Real-Redis interleavings: poll for the observable state instead of
  // sleeping a fixed amount, which is racy on slow machines.
  const waitFor = async (
    cond: () => Promise<boolean>,
    timeoutMs = 5000
  ): Promise<void> => {
    const start = Date.now()
    while (!(await cond())) {
      if (Date.now() - start > timeoutMs) {
        throw new Error("Timed out waiting for condition")
      }
      await setTimeout(10)
    }
  }

  it("should not revoke a lease another call re-entered, even mid-rollback", async () => {
    // A different owner holds the key the failing call will spin on.
    await provider.acquire("z_blocked", { ownerId: "other", expire: 10 })

    const cancellation = { cancelled: false }
    const failing = provider.acquire_(
      ["a_shared", "z_blocked"],
      { ownerId: "owner_race", awaitQueue: true, expire: 10 },
      cancellation
    )

    // Wait until the call holds "a_shared" and is spinning on "z_blocked".
    await waitFor(
      async () => (await client.get(`${prefix}a_shared`)) === "owner_race"
    )

    // Same owner re-enters "a_shared": this call registers its own claim
    // and from now on relies on the lease.
    await provider.acquire("a_shared", { ownerId: "owner_race", expire: 10 })

    // The first call now fails; its rollback withdraws only its own claim
    // and must leave the re-entered lease in place.
    cancellation.cancelled = true
    await expect(failing).rejects.toThrow(
      `Failed to acquire lock for key "z_blocked"`
    )

    expect(await client.get(`${prefix}a_shared`)).toBe("owner_race")

    expect(
      await provider.release("a_shared", { ownerId: "owner_race" })
    ).toBe(true)
    expect(await provider.release("z_blocked", { ownerId: "other" })).toBe(
      true
    )
  })

  it("should release a key when the creator and a re-entrant both fail", async () => {
    // Two concurrent multi-key calls share an owner and a key; the second
    // re-enters what the first freshly acquired, then BOTH fail. With a
    // single rotating token the first rollback went stale and the second
    // had nothing to withdraw, leaking the key until its TTL - forever at
    // ttl=0. With per-call claims the last withdrawal deletes the lease.
    await provider.acquire("z_both", { ownerId: "other_2", expire: 10 })

    const tokenKey = `medusa_lock_token:{${prefix}a_both}:${prefix}a_both`
    const c1 = { cancelled: false }
    const first = provider.acquire_(
      ["a_both", "z_both"],
      { ownerId: "owner_both", awaitQueue: true, expire: 10 },
      c1
    )
    await waitFor(
      async () => (await client.get(`${prefix}a_both`)) === "owner_both"
    )

    const c2 = { cancelled: false }
    const second = provider.acquire_(
      ["a_both", "z_both"],
      { ownerId: "owner_both", awaitQueue: true, expire: 10 },
      c2
    )
    // The second call re-enters "a_both": two claims registered.
    await waitFor(async () => (await client.hlen(tokenKey)) === 2)

    c1.cancelled = true
    await expect(first).rejects.toThrow(
      `Failed to acquire lock for key "z_both"`
    )
    // The creator's withdrawal must not delete the lease: the re-entrant
    // still claims it.
    expect(await client.get(`${prefix}a_both`)).toBe("owner_both")
    expect(await client.hlen(tokenKey)).toBe(1)

    c2.cancelled = true
    await expect(second).rejects.toThrow(
      `Failed to acquire lock for key "z_both"`
    )
    // The last claim is gone; so is the lock. No leak.
    expect(await client.get(`${prefix}a_both`)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)

    expect(await provider.release("z_both", { ownerId: "other_2" })).toBe(
      true
    )
  })

  it("should answer a replayed acquire with its original class", async () => {
    // ioredis retransmits commands whose replies were lost to a reconnect.
    // The script must answer a replay by its registered claim, not treat it
    // as a foreign holder - and a replayed rollback must stay withdrawn.
    const keyName = `${prefix}replay`
    const tokenKey = `medusa_lock_token:{${keyName}}:${keyName}`

    expect(
      await client.acquireLock(keyName, tokenKey, "owner_r", 10, "tok_a")
    ).toBe(1)
    // Replay of the fresh acquisition: still class 1.
    expect(
      await client.acquireLock(keyName, tokenKey, "owner_r", 10, "tok_a")
    ).toBe(1)

    // A second call re-enters - twice, simulating a replay: class 2 both
    // times.
    expect(
      await client.acquireLock(keyName, tokenKey, "owner_r", 10, "tok_b")
    ).toBe(2)
    expect(
      await client.acquireLock(keyName, tokenKey, "owner_r", 10, "tok_b")
    ).toBe(2)

    // Withdrawing the creator's claim keeps the lease: the re-entrant is
    // still live.
    expect(
      await client.rollbackLock(keyName, tokenKey, "owner_r", "tok_a")
    ).toBe(0)
    expect(await client.get(keyName)).toBe("owner_r")
    // A replayed rollback withdraws nothing further.
    expect(
      await client.rollbackLock(keyName, tokenKey, "owner_r", "tok_a")
    ).toBe(0)
    // The last claim's withdrawal deletes the lease.
    expect(
      await client.rollbackLock(keyName, tokenKey, "owner_r", "tok_b")
    ).toBe(1)
    expect(await client.get(keyName)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)
  })

  it("should keep the claim hash bounded under repeated refreshes", async () => {
    // Heartbeat pattern: the same owner re-acquires to refresh the TTL.
    // Every successful call collapses its transient claim into the single
    // durable 'held' marker instead of leaving one field per refresh.
    // Commits are fire-and-forget, so poll for the collapsed state.
    const tokenKey = `medusa_lock_token:{${prefix}beat}:${prefix}beat`
    for (let i = 0; i < 5; i++) {
      await provider.acquire("beat", { ownerId: "owner_hb", expire: 10 })
    }

    await waitFor(async () => (await client.hlen(tokenKey)) === 1)
    expect(await client.hget(tokenKey, "held")).toBe("1")

    expect(await provider.release("beat", { ownerId: "owner_hb" })).toBe(true)
    expect(await client.exists(tokenKey)).toBe(0)
  })

  it("should not delete a pre-claim-tracking lock its re-entrant failed on", async () => {
    // A lock written before claim tracking existed has no sidecar hash. The
    // code-2 re-entry seeds the durable marker for that unknown holder, so
    // the re-entrant's failure cannot withdraw the only claim and delete a
    // lock that predates the call.
    await client.set(`${prefix}legacy`, "owner_legacy")
    await provider.acquire("z_lgc", { ownerId: "other_3", expire: 10 })

    const tokenKey = `medusa_lock_token:{${prefix}legacy}:${prefix}legacy`
    const cancellation = { cancelled: false }
    const failing = provider.acquire_(
      ["legacy", "z_lgc"],
      { ownerId: "owner_legacy", awaitQueue: true, expire: 10 },
      cancellation
    )
    // Seeded 'held' marker + the re-entrant's transient claim.
    await waitFor(async () => (await client.hlen(tokenKey)) === 2)

    cancellation.cancelled = true
    await expect(failing).rejects.toThrow(
      `Failed to acquire lock for key "z_lgc"`
    )

    expect(await client.get(`${prefix}legacy`)).toBe("owner_legacy")

    await client.del(`${prefix}legacy`, tokenKey)
    expect(await provider.release("z_lgc", { ownerId: "other_3" })).toBe(true)
  })

  it("should fail an in-flight call whose key the owner released mid-flight", async () => {
    // A holds k committed; B (same owner) re-enters k while spinning on a
    // key someone else holds. Release is owner-final and deletes k
    // outright - B must then fail at its awaited commit's lease
    // verification instead of resolving without the lock.
    await provider.acquire("k_rel", { ownerId: "owner_rel", expire: 10 })
    await provider.acquire("z_rel", { ownerId: "other_4", expire: 10 })

    const tokenKey = `medusa_lock_token:{${prefix}k_rel}:${prefix}k_rel`
    await waitFor(async () => (await client.hget(tokenKey, "held")) === "1")

    const cancellation = { cancelled: false }
    const inflight = provider.acquire_(
      ["k_rel", "z_rel"],
      { ownerId: "owner_rel", awaitQueue: true, expire: 10 },
      cancellation
    )
    await waitFor(async () => (await client.hlen(tokenKey)) === 2)

    // The owner releases: key and sidecar fall immediately.
    expect(await provider.release("k_rel", { ownerId: "owner_rel" })).toBe(
      true
    )
    expect(await client.get(`${prefix}k_rel`)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)

    // The in-flight call cannot resolve lockless: whether it fails on the
    // blocked key or reaches its commit, the outcome is a clean failure and
    // a no-op rollback on the vanished key.
    cancellation.cancelled = true
    await expect(inflight).rejects.toThrow(/Failed to acquire lock|Timed-out/)
    expect(await client.get(`${prefix}k_rel`)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)

    expect(await provider.release("z_rel", { ownerId: "other_4" })).toBe(true)
  })

  it("should fail a stale call's commit even against a re-acquired same-owner lease", async () => {
    // The exact hole a 'held' fallback would open: A releases, then a NEW
    // same-owner call acquires and finalizes (fresh sidecar, 'held' only).
    // A stale in-flight call's mark finds its token gone and must report
    // 0 - not pass verification against a lease it never acquired.
    const keyName = `${prefix}verify`
    const tokenKey = `medusa_lock_token:{${keyName}}:${keyName}`
    expect(
      await client.acquireLock(keyName, tokenKey, "owner_v", 0, "tok_stale")
    ).toBe(1)
    expect(await provider.release("verify", { ownerId: "owner_v" })).toBe(true)

    // New same-owner lease, fully settled.
    await provider.acquire("verify", { ownerId: "owner_v", expire: 10 })
    await waitFor(async () => (await client.hget(tokenKey, "held")) === "1")

    expect(
      await client.commitLock(keyName, tokenKey, "owner_v", "tok_stale")
    ).toBe(0)
    // The stale call's rollback cannot revoke the new lease either.
    expect(
      await client.rollbackLock(keyName, tokenKey, "owner_v", "tok_stale")
    ).toBe(0)
    expect(await client.get(keyName)).toBe("owner_v")

    expect(await provider.release("verify", { ownerId: "owner_v" })).toBe(true)
  })

  it("should let the owner clean up any lost bookkeeping residue", async () => {
    // Crash orphans of every stage - a transient claim whose call died
    // pre-commit, or a committed mark whose fire-and-forget fold was lost -
    // are removed by the owner's stateless release, so no residue can lock
    // a ttl=0 key forever.
    const keyName = `${prefix}orphan`
    const tokenKey = `medusa_lock_token:{${keyName}}:${keyName}`
    expect(
      await client.acquireLock(keyName, tokenKey, "owner_o", 0, "tok_lost")
    ).toBe(1)
    expect(
      await client.commitLock(keyName, tokenKey, "owner_o", "tok_lost")
    ).toBe(1)
    // No finalizeLock call - simulates the lost fold.
    expect(await client.hget(tokenKey, "tok_lost")).toBe("3")

    expect(await provider.release("orphan", { ownerId: "owner_o" })).toBe(true)
    expect(await client.get(keyName)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)
  })

  it("should end every hold of the owner when the owner releases", async () => {
    // Owner-scoped semantics, pinned deliberately: the public API
    // identifies a hold by (key, owner) alone, so two committed same-owner
    // holders are one hold to release - indistinguishable even in
    // principle, since a heartbeat TTL refresh and a concurrent call issue
    // identical commands. An early owner release ends both, exactly as it
    // always did; an in-flight call is protected from false success by its
    // awaited commit, not by the release.
    await provider.acquire("k_own", { ownerId: "owner_own", expire: 10 })
    const tokenKey = `medusa_lock_token:{${prefix}k_own}:${prefix}k_own`
    await waitFor(async () => (await client.hget(tokenKey, "held")) === "1")

    // A second same-owner call re-enters and commits successfully: both
    // acknowledged holds share the durable marker.
    await provider.acquire("k_own", { ownerId: "owner_own", expire: 10 })
    await waitFor(async () => (await client.hlen(tokenKey)) === 1)

    // The owner releases: the shared hold ends.
    expect(await provider.release("k_own", { ownerId: "owner_own" })).toBe(
      true
    )
    expect(await client.get(`${prefix}k_own`)).toBeNull()
    expect(await client.exists(tokenKey)).toBe(0)

    // The other call's own release finds nothing left and reports false,
    // harmlessly.
    expect(await provider.release("k_own", { ownerId: "owner_own" })).toBe(
      false
    )
  })

  it("should name sidecars injectively and slot-aligned with their lock key", async () => {
    // Naming lives inside the Lua scripts; a real Redis is the only place
    // it is observable. The sidecar's first brace pair is the lock key's
    // own hash tag when it has one, otherwise the whole lock key name, so
    // both keys hash to the same cluster slot.
    await provider.acquire("plain", { ownerId: "owner_race", expire: 10 })
    await provider.acquire("{tenant_1}:cart", {
      ownerId: "owner_race",
      expire: 10,
    })

    expect(
      await client.exists(`medusa_lock_token:{${prefix}plain}:${prefix}plain`)
    ).toBe(1)
    expect(
      await client.exists(
        `medusa_lock_token:{tenant_1}:${prefix}{tenant_1}:cart`
      )
    ).toBe(1)

    expect(
      await provider.release(["plain", "{tenant_1}:cart"], {
        ownerId: "owner_race",
      })
    ).toBe(true)
    expect(
      await client.exists(`medusa_lock_token:{${prefix}plain}:${prefix}plain`)
    ).toBe(0)
  })

  it("should not roll back a key that expired and was re-acquired anonymously", async () => {
    // The S10 scenario through the public API: an anonymous multi-key call
    // loses a key mid-flight (expiry simulated with DEL of lock + sidecar,
    // which share a lifetime) and another anonymous caller re-acquires it.
    // Both holders are "*", but the tokens differ, so the failing call's
    // rollback must leave the new holder's lease alone.
    await provider.acquire("z_usurp", { ownerId: "blocker", expire: 10 })

    const cancellation = { cancelled: false }
    const failing = provider.acquire_(
      ["a_usurp", "z_usurp"],
      { awaitQueue: true, expire: 10 },
      cancellation
    )

    // Wait until the call holds "a_usurp" and is spinning on "z_usurp".
    await waitFor(async () => (await client.get(`${prefix}a_usurp`)) === "*")

    // Simulated expiry, then anonymous re-acquisition by someone else.
    await client.del(
      `${prefix}a_usurp`,
      `medusa_lock_token:{${prefix}a_usurp}:${prefix}a_usurp`
    )
    await provider.acquire("a_usurp", { expire: 10 })

    cancellation.cancelled = true
    await expect(failing).rejects.toThrow(
      `Failed to acquire lock for key "z_usurp"`
    )

    // The usurper's anonymous lease survived the stale rollback.
    expect(await client.get(`${prefix}a_usurp`)).toBe("*")

    expect(await provider.release("a_usurp")).toBe(true)
    expect(await provider.release("z_usurp", { ownerId: "blocker" })).toBe(
      true
    )
  })
})
