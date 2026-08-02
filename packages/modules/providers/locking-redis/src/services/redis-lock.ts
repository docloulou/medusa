import { MedusaError, promiseAll } from "@medusajs/framework/utils"
import { randomUUID } from "node:crypto"
import { ILockingProvider } from "@medusajs/types"
import { RedisCacheModuleOptions } from "@types"
import { Redis } from "ioredis"
import type { ChainableCommander } from "ioredis"
import { setTimeout } from "node:timers/promises"

/**
 * Namespace for the sidecar keys recording which acquisition created a
 * lease. It deliberately does not extend `keyNamePrefix`, so releaseAll's
 * SCAN over `${keyNamePrefix}*` never sees these keys; they are created,
 * refreshed, and deleted exclusively alongside their lock key by the Lua
 * scripts below, and always declared to them as KEYS[2].
 */
const TOKEN_KEY_NAMESPACE = "medusa_lock_token:"

export class RedisLockingProvider implements ILockingProvider {
  static identifier = "locking-redis"

  protected redisClient: Redis & {
    /**
     * Returns 1 when the lock was freshly acquired, 2 when the calling owner
     * already held it and the TTL was refreshed, and 0 when it is held by
     * someone else. `token` identifies this exact acquisition call: the
     * declared sidecar key holds a hash of claim tokens (one field per call
     * that holds this lease, valued '1' for the creator and '2' for a
     * re-entrant) with the same lifetime as the lock, so a failed multi-key
     * call can later withdraw exactly the claims it registered.
     */
    acquireLock: (
      key: string,
      tokenKey: string,
      ownerId: string,
      ttl: number,
      token: string
    ) => Promise<number | string>
    /**
     * Releases the owner's lock, deleting the key and its sidecar outright.
     * Owner-final and stateless: claim bookkeeping never narrows a release.
     * An in-flight call whose key is released mid-flight is caught by its
     * own awaited commit, which re-verifies the lease and fails the call;
     * an acknowledged hold may fall to owner action or TTL at any time,
     * and a same-owner release racing an acquire serializes as
     * acquire-then-release.
     */
    releaseLock: (
      key: string,
      tokenKey: string,
      ownerId: string
    ) => Promise<number | string>
    /**
     * Withdraws this call's claim on the key. The key is deleted only when
     * the claim was still registered (a key that expired and was re-acquired
     * - even by the same owner - carries a fresh claim hash this token never
     * appears in) and no other call's claim survives it; otherwise the lease
     * is left alone for its remaining holders.
     */
    rollbackLock: (
      key: string,
      tokenKey: string,
      ownerId: string,
      token: string
    ) => Promise<number | string>
    /**
     * Phase one of the commit, awaited: marks this call's claim committed
     * ('3') while keeping its token identity, so the marks stay reversible
     * by token until every key's mark is known durable. Returns 0 when the
     * lease is no longer this owner's OR this call's claim is gone from
     * the sidecar - the call's last-line verification; it never accepts
     * another lease's 'held' as its own.
     */
    commitLock: (
      key: string,
      tokenKey: string,
      ownerId: string,
      token: string
    ) => Promise<number | string>
    /**
     * Phase two, fire-and-forget: folds this call's own committed mark into
     * the shared 'held' marker so repeated same-owner refreshes cannot grow
     * the claim hash. Only run after every mark of the call landed; a lost
     * fold leaves the '3' mark, which release clears.
     */
    finalizeLock: (
      key: string,
      tokenKey: string,
      ownerId: string,
      token: string
    ) => Promise<number | string>
  }
  protected keyNamePrefix: string
  protected waitLockingTimeout: number = 5
  protected defaultRetryInterval: number = 20
  protected maximumRetryInterval: number = 1000
  protected backoffFactor: number = 2

  constructor({ redisClient, prefix }, options: RedisCacheModuleOptions) {
    this.redisClient = redisClient
    this.keyNamePrefix = prefix ?? "medusa_lock:"

    // A lock key is `keyNamePrefix + <logical name>`, with the logical name
    // unconstrained. The only way to guarantee no lock key can ever spell
    // out a sidecar token key (and corrupt or be corrupted by it) is for
    // the two namespaces to be prefix-disjoint. The default "medusa_lock:"
    // is; a custom prefix that overlaps is a misconfiguration.
    if (
      TOKEN_KEY_NAMESPACE.startsWith(this.keyNamePrefix) ||
      this.keyNamePrefix.startsWith(TOKEN_KEY_NAMESPACE)
    ) {
      throw new MedusaError(
        MedusaError.Types.INVALID_ARGUMENT,
        `The locking key prefix "${this.keyNamePrefix}" overlaps the "${TOKEN_KEY_NAMESPACE}" namespace reserved for acquisition tokens. Configure a prefix that neither starts with it nor is a prefix of it.`
      )
    }

    // ioredis `keyPrefix` silently rewrites every declared KEYS[n] but not
    // SCAN patterns or results, so releaseAll and the slot alignment between
    // a lock key and its sidecar would both break. Reject it outright rather
    // than support it partially.
    const clientKeyPrefix =
      redisClient?.options?.keyPrefix ??
      redisClient?.options?.redisOptions?.keyPrefix
    if (clientKeyPrefix) {
      throw new MedusaError(
        MedusaError.Types.INVALID_ARGUMENT,
        `The Redis locking provider does not support an ioredis "keyPrefix" (got "${clientKeyPrefix}"). Namespace lock keys with the provider prefix option instead.`
      )
    }

    if (!isNaN(+options?.waitLockingTimeout!)) {
      this.waitLockingTimeout = +options.waitLockingTimeout!
    }

    if (!isNaN(+options?.defaultRetryInterval!)) {
      this.defaultRetryInterval = +options.defaultRetryInterval!
    }

    if (!isNaN(+options?.maximumRetryInterval!)) {
      this.maximumRetryInterval = +options.maximumRetryInterval!
    }

    if (!isNaN(+options?.backoffFactor!)) {
      this.backoffFactor = +options.backoffFactor!
    }

    // Define the custom command for acquiring locks. KEYS[2] declares the
    // sidecar token key, as the scripting contract requires for every key a
    // script touches. Note for direct callers of these commands: the shapes
    // changed with the introduction of the sidecar (they are provider
    // internals, not public API).
    this.redisClient.defineCommand("acquireLock", {
      numberOfKeys: 2,
      lua: `
        local key = KEYS[1]
        local tokenKey = KEYS[2]
        local ownerId = ARGV[1]
        local ttl = tonumber(ARGV[2])
        local token = ARGV[3]

        local setArgs = {key, ownerId, 'NX'}
        if ttl > 0 then
            table.insert(setArgs, 'EX')
            table.insert(setArgs, ttl)
        end

        if redis.call('SET', unpack(setArgs)) then
            -- Register this call's claim on the fresh lease. The sidecar is
            -- a hash of claim tokens - one field per acquisition call that
            -- holds this lease - and lives and dies with the lock key inside
            -- these scripts. A stale hash left over from an expired lease
            -- must not linger under the new one.
            redis.call('DEL', tokenKey)
            redis.call('HSET', tokenKey, token, '1')
            if ttl > 0 then
                redis.call('EXPIRE', tokenKey, ttl)
            end
            return 1
        end

        -- ioredis retransmits commands whose replies were lost to a
        -- reconnect, so a call whose claim is already registered must be
        -- answered with its original class: misreading a replayed fresh
        -- acquisition as "held by someone else" would spin or fail a call
        -- that actually holds the lock.
        local class = redis.call('HGET', tokenKey, token)
        if class == '1' then
            return 1
        elseif class == '2' then
            return 2
        end

        -- The key is already held. A named owner re-entering its own lock
        -- refreshes the TTL and reports 2, so the caller can tell a fresh
        -- acquisition from a re-entrant one and never rolls back a lock it
        -- already held before the call. '*' is the anonymous owner: two
        -- anonymous holders are distinct, so it never re-enters.
        -- This check used to be skipped entirely when awaitQueue was set,
        -- which made an owner spin on its own lock until the key expired
        -- (or indefinitely when it had no TTL).
        local currentOwnerId = redis.call('GET', key)
        if currentOwnerId == ownerId and currentOwnerId ~= '*' then
            setArgs = {key, ownerId, 'XX'}
            if ttl > 0 then
                table.insert(setArgs, 'EX')
                table.insert(setArgs, ttl)
            end
            redis.call('SET', unpack(setArgs))
            -- Register a re-entrant claim alongside the creator's. From here
            -- on the lease is load-bearing for this caller too: a failing
            -- call only ever withdraws its own claim, and the lock is
            -- deleted only when the last registered claim is withdrawn, so
            -- after a successful re-entry no failure path can revoke the
            -- lease while any claimant is still live.
            -- A code-2 re-entry proves someone already held this lease. If
            -- the claim hash is empty - a lock written before claim
            -- tracking existed, or altered out-of-band - seed the durable
            -- 'held' marker for that unknown holder first; otherwise this
            -- call's failure would withdraw the only claim and delete a
            -- pre-held lock.
            if redis.call('HLEN', tokenKey) == 0 then
                redis.call('HSET', tokenKey, 'held', '1')
            end
            redis.call('HSET', tokenKey, token, '2')
            if ttl > 0 then
                redis.call('EXPIRE', tokenKey, ttl)
            else
                redis.call('PERSIST', tokenKey)
            end
            return 2
        end

        return 0
      `,
    })

    // Define the custom command for releasing an owner's lock. Owner-final
    // and stateless by design: (key, owner) is the public API's hold
    // identity, so the owner's release deletes the key and its sidecar
    // outright - claim bookkeeping never narrows it. A call still in
    // flight on this key cannot be handed a false success by that: its
    // awaited commit re-verifies the lease and fails the call when the
    // lease is gone. A commit that was already acknowledged may fall to
    // owner action or TTL at any time - including a call's resolve window;
    // no protocol can make resolving atomic with concurrent
    // owner-authorized deletions, and a same-owner release racing an
    // acquire serializes as acquire-then-release. Replayed or duplicated
    // releases find no key and report 0, changing nothing.
    this.redisClient.defineCommand("releaseLock", {
      numberOfKeys: 2,
      lua: `
        local key = KEYS[1]
        local tokenKey = KEYS[2]
        local ownerId = ARGV[1]

        if redis.call('GET', key) ~= ownerId then
            return 0
        end

        redis.call('UNLINK', tokenKey)
        return redis.call('DEL', key)
      `,
    })

    // Define the custom command for withdrawing a failed call's claim on a
    // key and deleting the lease when no claim survives it.
    this.redisClient.defineCommand("rollbackLock", {
      numberOfKeys: 2,
      lua: `
        local key = KEYS[1]
        local tokenKey = KEYS[2]
        local ownerId = ARGV[1]
        local token = ARGV[2]

        -- Withdraw this call's claim first: even when the lock must
        -- survive, a failed call no longer counts among its holders.
        local removed = redis.call('HDEL', tokenKey, token)

        if redis.call('GET', key) ~= ownerId then
            return 0
        end

        -- Delete only a lease this failed call verifiably held - its claim
        -- was still registered; a key that expired and was re-acquired
        -- carries a fresh hash this token never appears in - and that no
        -- other live call still relies on.
        if removed == 1 and redis.call('HLEN', tokenKey) == 0 then
            redis.call('UNLINK', tokenKey)
            return redis.call('UNLINK', key)
        end
        return 0
      `,
    })

    // Define the custom command for phase one of the commit, awaited by
    // acquire_: a resolved acquire guarantees its holds are durably
    // recorded, and a commit finding the lease gone fails the call instead
    // of letting it resolve under a lock it no longer owns.
    this.redisClient.defineCommand("commitLock", {
      numberOfKeys: 2,
      lua: `
        local key = KEYS[1]
        local tokenKey = KEYS[2]
        local ownerId = ARGV[1]
        local token = ARGV[2]

        -- Last-line lease verification: when the key expired or was
        -- released and re-acquired while this call was in flight, the call
        -- must report failure, not resolve under a lease it no longer
        -- holds.
        if redis.call('GET', key) ~= ownerId then
            return 0
        end

        -- Mark this call's claim committed, keeping its token identity: a
        -- '3' on one key says nothing about the call's other keys, so the
        -- mark must stay reversible by token until the caller has seen
        -- every mark land. Only the owning call may fold its mark into
        -- 'held' (finalizeLock), strictly after every mark settled - so a
        -- replayed mark always still finds its token and rewrites '3'
        -- harmlessly, and the token's absence is unambiguous: the hash was
        -- rebuilt without this call's claim (release, or expiry plus
        -- re-acquisition - even by the same owner). Its lease continuity
        -- is gone and the call must fail; falling back to another lease's
        -- 'held' would let a stale call pass verification against a lease
        -- it never acquired.
        if redis.call('HEXISTS', tokenKey, token) == 1 then
            redis.call('HSET', tokenKey, token, '3')
            return 1
        end
        return 0
      `,
    })

    // Define the custom command for phase two of the commit, fired and
    // forgotten once every mark of the call has landed: fold this call's
    // own committed mark into the shared 'held' marker so repeated
    // same-owner refreshes cannot grow the claim hash. A lost fold leaves
    // the '3' mark, which release clears; the guard keeps a replayed fold
    // from resurrecting state a release already removed.
    this.redisClient.defineCommand("finalizeLock", {
      numberOfKeys: 2,
      lua: `
        local key = KEYS[1]
        local tokenKey = KEYS[2]
        local ownerId = ARGV[1]
        local token = ARGV[2]

        if redis.call('GET', key) ~= ownerId then
            return 0
        end

        -- HSET before HDEL: removing the last field deletes the hash and
        -- its TTL, and a re-created hash would be a permanent orphan after
        -- the lock expires. In this order the hash is never empty, so its
        -- TTL survives the swap.
        if redis.call('HGET', tokenKey, token) == '3' then
            redis.call('HSET', tokenKey, 'held', '1')
            redis.call('HDEL', tokenKey, token)
        end
        return 1
      `,
    })
  }

  private getKeyName(key: string): string {
    return `${this.keyNamePrefix}${key}`
  }

  /**
   * Sidecar name for a lock key, or null when none can share its slot.
   *
   * Two constraints pin this name down:
   * - It must hash to the same cluster slot as its lock key, or the
   *   multi-key scripts above fail with CROSSSLOT on cluster-mode or
   *   proxied Redis. The namespace contains no braces, so the `{slotTag}`
   *   below is always the sidecar's first brace pair: the lock key's own
   *   hash tag when it has a valid one, otherwise the whole lock key name
   *   (which is then also exactly what Redis hashes for the lock key).
   * - It must be injective. The full lock key name is appended outside
   *   the tag, and since a tag never contains '}', the encoding parses
   *   back unambiguously - two distinct lock keys can never share a
   *   sidecar, no matter what braces they contain.
   *
   * A lock key containing a stray '}' without a valid hash tag cannot be
   * wrapped this way ('}' would terminate the tag early), so no aligned
   * sidecar exists for it: this returns null and acquisition rejects the
   * key. Such keys already could not participate in any multi-key
   * operation on cluster-mode Redis.
   */
  private tokenKeyNameFor(keyName: string): string | null {
    const open = keyName.indexOf("{")
    const close = open >= 0 ? keyName.indexOf("}", open + 1) : -1
    const hasValidTag = close > open + 1

    if (hasValidTag || !keyName.includes("}")) {
      const slotTag = hasValidTag ? keyName.slice(open + 1, close) : keyName
      return `${TOKEN_KEY_NAMESPACE}{${slotTag}}:${keyName}`
    }

    return null
  }

  private getTokenKeyName(keyName: string): string {
    const tokenKeyName = this.tokenKeyNameFor(keyName)

    if (tokenKeyName === null) {
      throw new MedusaError(
        MedusaError.Types.INVALID_ARGUMENT,
        `Invalid lock key "${keyName}": it contains '}' without a valid hash tag, so no acquisition-token key can share its hash slot.`
      )
    }

    return tokenKeyName
  }

  async execute<T>(
    keys: string | string[],
    job: () => Promise<T>,
    args?: {
      timeout?: number
    }
  ): Promise<T> {
    const timeout = Math.max(args?.timeout ?? this.waitLockingTimeout, 1)
    const timeoutSeconds = Number.isNaN(timeout) ? 1 : timeout

    const cancellationToken = { cancelled: false }
    const promises: Promise<any>[] = []
    if (timeoutSeconds > 0) {
      promises.push(this.getTimeout(timeoutSeconds, cancellationToken))
    }

    const ONE_MINUTE = 60
    promises.push(
      this.acquire_(
        keys,
        {
          awaitQueue: true,
          expire: args?.timeout ? timeoutSeconds : ONE_MINUTE,
        },
        cancellationToken
      )
    )

    await Promise.race(promises)

    try {
      return await job()
    } finally {
      await this.release(keys)
    }
  }

  async acquire(
    keys: string | string[],
    args?: {
      ownerId?: string
      expire?: number
      awaitQueue?: boolean
    }
  ): Promise<void> {
    return this.acquire_(keys, args)
  }

  async acquire_(
    keys: string | string[],
    args?: {
      ownerId?: string
      expire?: number
      awaitQueue?: boolean
    },
    cancellationToken?: { cancelled: boolean; acquired?: boolean }
  ): Promise<void> {
    keys = Array.isArray(keys) ? keys : [keys]

    const timeout = Math.max(args?.expire ?? this.waitLockingTimeout, 1)
    const timeoutSeconds = Number.isNaN(timeout) ? 1 : timeout

    const ownerId = args?.ownerId ?? "*"
    const awaitQueue = args?.awaitQueue ?? false
    const ttl = args?.expire ? timeoutSeconds : 0

    // Acquire in a stable order, one key at a time. Two callers asking for the
    // same set of keys queue behind each other instead of deadlocking on an
    // interleaved order, and a failure can undo what this call already took.
    const orderedKeys = [...new Set(keys)].sort()

    // One token per call: every key this call touches registers the token as
    // a claim in the key's sidecar hash, so the rollback below can withdraw
    // exactly this call's claims and nothing else.
    const token = randomUUID()

    // Keys this call holds a claim on: freshly acquired (1) and re-entered
    // (2) alike. Which withdrawal actually deletes a lease is decided
    // atomically in the rollback script - only the last surviving claim
    // takes the lock down with it.
    const claimed: string[] = []

    try {
      for (const key of orderedKeys) {
        const errMessage = `Failed to acquire lock for key "${key}"`
        const keyName = this.getKeyName(key)
        let retryDelay = this.defaultRetryInterval

        while (true) {
          if (cancellationToken?.cancelled) {
            throw new MedusaError(MedusaError.Types.CONFLICT, errMessage)
          }

          // Clients configured with ioredis `stringNumbers` deliver Lua
          // integer replies as strings; normalize before dispatching.
          const result = Number(
            await this.redisClient.acquireLock(
              keyName,
              this.getTokenKeyName(keyName),
              ownerId,
              ttl,
              token
            )
          )

          if (result === 1 || result === 2) {
            claimed.push(key)

            // The cancellation may have fired while the script call was in
            // flight (execute's timeout races the last acquisition). Bailing
            // out only after recording the claim keeps a just-acquired lock
            // inside this call's rollback instead of leaking it.
            if (cancellationToken?.cancelled) {
              throw new MedusaError(MedusaError.Types.CONFLICT, errMessage)
            }
            break
          }

          if (!awaitQueue) {
            throw new MedusaError(MedusaError.Types.CONFLICT, errMessage)
          }

          // Wait before retrying with exponential backoff and jitter
          const jitteredDelay = retryDelay * (0.5 + Math.random() * 0.5)
          await setTimeout(jitteredDelay)

          retryDelay = Math.min(
            retryDelay * this.backoffFactor,
            this.maximumRetryInterval
          )
        }
      }

      // Every key is acquired. While the marks below are in flight the
      // claims stay reversible by token, so a timeout firing mid-commit may
      // still reject the call: it falls into the rollback like any other
      // failure. The race with execute()'s timeout is settled only after
      // every mark has landed.
      if (cancellationToken?.cancelled) {
        throw new MedusaError(
          MedusaError.Types.CONFLICT,
          "Timed-out acquiring lock."
        )
      }

      // Phase one of the commit, awaited: mark every claim this call took
      // as committed ('3'), keeping token identity so the marks stay
      // reversible until the whole set is known durable. This doubles as
      // the call's last-line verification: a mark finding the lease gone
      // (expired, or released and re-acquired while this call was in
      // flight) fails the call, so it never resolves under a lease it no
      // longer holds - release itself stays owner-final and stateless.
      // Rejected marks mean Redis went unreachable on this connection; the
      // rollback below is then best-effort like every path under a
      // partition, and whatever it cannot reach stays owner-releasable.
      const marked = await Promise.allSettled(
        claimed.map((key) => {
          const keyName = this.getKeyName(key)
          return this.redisClient.commitLock(
            keyName,
            this.getTokenKeyName(keyName),
            ownerId,
            token
          )
        })
      )

      if (
        marked.some((m) => m.status !== "fulfilled" || Number(m.value) !== 1)
      ) {
        throw new MedusaError(
          MedusaError.Types.CONFLICT,
          `Failed to acquire lock for key${
            claimed.length > 1 ? "s" : ""
          } "${claimed.join('", "')}"`
        )
      }

      if (cancellationToken) {
        if (cancellationToken.cancelled) {
          throw new MedusaError(
            MedusaError.Types.CONFLICT,
            "Timed-out acquiring lock."
          )
        }
        cancellationToken.acquired = true
      }

      // Phase two, fire-and-forget once every mark has landed: fold this
      // call's own marks into the shared 'held' marker so repeated
      // same-owner refreshes cannot grow the claim hash. Only the owning
      // call may fold its mark - another call cannot tell a mid-flight '3'
      // from an acknowledged one - and a lost fold is harmless: the '3'
      // keeps the lease alive exactly like 'held' until release or expiry
      // removes the sidecar wholesale.
      for (const key of claimed) {
        const keyName = this.getKeyName(key)
        void this.redisClient
          .finalizeLock(keyName, this.getTokenKeyName(keyName), ownerId, token)
          .catch(() => {})
      }
    } catch (error) {
      // Never leave a partially acquired set behind. Without this, the keys
      // taken before the failure stay held until their TTL expires - and
      // forever when no expiry was requested, since the script then issues
      // SET NX without EX. The rollback is best-effort: the acquisition error
      // is always the one that propagates.
      //
      // The rollback withdraws this call's claim from every key it touched.
      // A lock is deleted only when its last registered claim is withdrawn:
      // a lease another live call (creator or re-entrant) still claims
      // survives, while a lease whose every claimant failed is removed even
      // when this call merely re-entered it. A key that expired mid-call and
      // was re-acquired by anyone - even this owner - carries a fresh claim
      // hash this token never appears in, so it is left alone; that also
      // makes anonymous ("*") rollbacks safe.
      if (claimed.length) {
        await promiseAll(
          claimed.map((key) => {
            const keyName = this.getKeyName(key)
            return this.redisClient
              .rollbackLock(
                keyName,
                this.getTokenKeyName(keyName),
                ownerId,
                token
              )
              .catch(() => {})
          })
        ).catch(() => {})
      }

      throw error
    }
  }

  async release(
    keys: string | string[],
    args?: {
      ownerId?: string | null
    }
  ): Promise<boolean> {
    const ownerId = args?.ownerId ?? "*"
    keys = Array.isArray(keys) ? keys : [keys]

    const releasePromises = keys.map(async (key) => {
      const keyName = this.getKeyName(key)
      // Number(): clients configured with ioredis `stringNumbers` deliver
      // Lua integer replies as strings.
      const result = Number(
        await this.redisClient.releaseLock(
          keyName,
          this.getTokenKeyName(keyName),
          ownerId
        )
      )
      return result === 1
    })

    const results = await promiseAll(releasePromises)

    return results.every((released) => released)
  }

  async releaseAll(args?: { ownerId?: string | null }): Promise<void> {
    const ownerId = args?.ownerId ?? "*"

    const pattern = `${this.keyNamePrefix}*`
    let cursor = "0"
    const unreleasable: string[] = []

    do {
      const result = await this.redisClient.scan(
        cursor,
        "MATCH",
        pattern,
        "COUNT",
        100
      )
      cursor = result[0]
      const keys = result[1]

      if (keys.length > 0) {
        // Compare-and-delete every key in a single round trip. Reading the
        // owners first and deleting them in a second pipeline let a key expire
        // and be re-acquired in between, so the delete destroyed the new
        // owner's lock. The releaseLock script already does this atomically.
        const pipeline = this.redisClient.pipeline() as ChainableCommander & {
          releaseLock: (
            key: string,
            tokenKey: string,
            ownerId: string
          ) => ChainableCommander
        }

        keys.forEach((key) => {
          const tokenKey = this.tokenKeyNameFor(key)
          // A scanned name containing '}' without a valid hash tag has no
          // slot-aligned sidecar, so the two-key script cannot target it
          // (CROSSSLOT on cluster). Release everything else, then fail
          // loudly below instead of reporting a clean sweep.
          if (tokenKey === null) {
            unreleasable.push(key)
            return
          }
          pipeline.releaseLock(key, tokenKey, ownerId)
        })

        // pipeline.exec resolves per-command [error, result] tuples instead
        // of rejecting, so command failures (ACL on the token namespace,
        // CROSSSLOT, script errors) would otherwise report a clean sweep.
        const results = await pipeline.exec()
        const firstError = results?.find(([err]) => err != null)?.[0]
        if (firstError) {
          throw new MedusaError(
            MedusaError.Types.UNEXPECTED_STATE,
            `releaseAll failed to release scanned lock(s): ${firstError.message}`
          )
        }
      }
    } while (cursor !== "0")

    if (unreleasable.length) {
      throw new MedusaError(
        MedusaError.Types.INVALID_ARGUMENT,
        `releaseAll released every other matching lock but could not release ${unreleasable.length} key(s) containing '}' without a valid hash tag (no acquisition-token key can share their hash slot): ${unreleasable.join(", ")}`
      )
    }
  }

  private async getTimeout(
    seconds: number,
    cancellationToken: { cancelled: boolean; acquired?: boolean }
  ): Promise<void> {
    await setTimeout(seconds * 1000)

    // The acquisition completed while this timer was pending: stand down.
    // Cancelling now could not free anything - the claims are already
    // committed (or about to be) - it would only strand held locks behind
    // a caller that was told it failed. Resolving is safe: every lock is
    // held, so execute() proceeds to its job, and the pending commits are
    // owner-guarded bookkeeping that no-op if the keys are released first.
    if (cancellationToken.acquired) {
      return
    }

    cancellationToken.cancelled = true
    throw new MedusaError(
      MedusaError.Types.CONFLICT,
      "Timed-out acquiring lock."
    )
  }
}
