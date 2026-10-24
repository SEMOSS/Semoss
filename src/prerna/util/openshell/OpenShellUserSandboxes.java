/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.util.openshell;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.om.ClientProcessWrapper;
import prerna.redis.RedisConnectionConfig;
import prerna.redis.RedisConnectionFactory;
import prerna.util.Constants;
import prerna.util.OpenShellTransport;
import prerna.util.OpenShellTransport.SandboxInfo;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/**
 * One OpenShell sandbox per user, shared by every session and server instance.
 *
 * <p>
 * The sandbox outlives any single session: its worker is persistent, so a server
 * instance that dies, restarts or is simply a different node leaves the user's python
 * process, its variables and its files in place, and the next request reattaches. The
 * sandbox is deleted when the user's <b>last</b> live session ends, wherever that
 * session lived.
 *
 * <p>
 * Three pieces of state, at two scopes:
 * <ul>
 * <li><b>Registry</b> (shared): owner &rarr; sandbox name, plus the set of the user's
 * live sessions, each with an expiry refreshed by every request (a lease equal to the
 * session timeout). Redis when configured; an in-memory map otherwise, which is only
 * correct for a single server instance.</li>
 * <li><b>Local</b> (this instance): one {@link SymlinkHelper} and one
 * {@link ClientProcessWrapper} per user, so a user's sessions on this instance share a
 * single connection and a single record of what was transported, instead of one forward
 * and one worker connection per session (or per cookie-less API call).</li>
 * <li><b>Sweeper</b> (this instance, periodic): deletes sandboxes whose owner has no live
 * session left, and labelled sandboxes that are no longer running.</li>
 * </ul>
 */
public final class OpenShellUserSandboxes {

	private static final Logger classLogger = LogManager.getLogger(OpenShellUserSandboxes.class);

	/** Session attribute holding the owner a session's lease was taken under. */
	public static final String SESSION_OWNER_ATTR = "OPENSHELL_SANDBOX_OWNER";
	/** How often a session may refresh its lease; requests in between are free. */
	private static final long TOUCH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(60);
	/** How long a creator may hold the per-user create lock. */
	private static final long CREATE_LOCK_MS = TimeUnit.MINUTES.toMillis(5);
	/** A labelled sandbox younger than this is never swept: its registry entry may not exist yet. */
	private static final Duration SWEEP_GRACE = Duration.ofMinutes(10);
	private static final long SWEEP_INTERVAL_MINUTES = 5;
	/** How long {@link #acquire} keeps trying: a create is bounded by the CLI's ready timeout. */
	private static final long ACQUIRE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(240);
	/** Pause between attempts while another instance creates, or the gateway does not answer. */
	private static final long ACQUIRE_POLL_MS = 1000;
	/** Failed attaches to a Ready sandbox before it is replaced; one failure may be a hiccup. */
	private static final int MAX_ATTACH_ATTEMPTS = 2;
	/** Session lease when the container gives no timeout (a negative interval never expires). */
	private static final long DEFAULT_LEASE_MS = TimeUnit.DAYS.toMillis(1);
	private static final DateTimeFormatter CREATED_AT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	/** Distinguishes this JVM's leases from ones a restored session carries over a restart. */
	private static final String BOOT_ID = UUID.randomUUID().toString();
	private static final String NODE = nodeName();

	private static final Map<String, Local> LOCALS = new ConcurrentHashMap<>();
	private static final Map<String, Set<String>> LOCAL_SESSIONS = new ConcurrentHashMap<>();
	private static final Object LOCALS_LOCK = new Object();
	/**
	 * Serialises, per user, the changes to which of their sessions live here and the release
	 * of their connection, so a session ending cannot tear down a wrapper another session
	 * of theirs just started using. Always taken before a {@link Local}'s monitor.
	 */
	private static final Map<String, Object> OWNER_LOCKS = new ConcurrentHashMap<>();
	private static volatile Registry registry;
	private static volatile ScheduledExecutorService sweeper;

	private OpenShellUserSandboxes() {
		// static utility
	}

	/** User-scoped sandboxes apply exactly when the OpenShell jail does. */
	public static boolean enabled() {
		return Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))
				&& Constants.SANDBOX_MODE_OPENSHELL.equalsIgnoreCase(
						String.valueOf(Utility.getDIHelperProperty(Constants.SANDBOX_MODE)).trim());
	}

	/**
	 * Whether this user's python lives in a user-scoped sandbox: the jail is OpenShell and
	 * the user has a primary login to scope it by. Anyone else keeps the per-session form.
	 */
	public static boolean appliesTo(User user) {
		if (user == null || !enabled()) {
			return false;
		}
		AccessToken token = user.getPrimaryLoginToken();
		return token != null && token.getId() != null;
	}

	/**
	 * The owner a session's sandbox lease was taken under, for cleanup when the session is
	 * destroyed. Read from the session first: logging out of a provider drops its token
	 * before the session is invalidated, so by then the user may have no login to derive
	 * the owner from, and the lease would never be released.
	 *
	 * @param recorded the session's {@link #SESSION_OWNER_ATTR} value, or null
	 * @return the owner, or null when this session has no user-scoped sandbox
	 */
	public static String ownerForSession(User user, Object recorded) {
		if (!enabled()) {
			return null;
		}
		if (recorded instanceof String owner && !owner.isEmpty()) {
			return owner;
		}
		return appliesTo(user) ? ownerOf(user) : null;
	}

	/**
	 * The digest a user's sandbox is filed under: provider and id of the primary login.
	 * Used as the registry key and the {@code semoss-owner} label, so neither carries the
	 * raw user id.
	 */
	public static String ownerOf(User user) {
		AccessToken token = user == null ? null : user.getPrimaryLoginToken();
		if (token == null || token.getId() == null) {
			throw new IllegalStateException("cannot scope a sandbox to a user with no primary login");
		}
		return digest(token.getProvider().name() + ":" + token.getId(), 24);
	}

	// ------------------------------------------------------------------ this instance

	/** What this instance holds for one user, shared by all of their sessions here. */
	public static final class Local {
		private final SymlinkHelper helper;
		private final ClientProcessWrapper cpw = new ClientProcessWrapper();
		/** Set under this object's monitor when the instance gives the user up. */
		private boolean released;

		private Local(SymlinkHelper helper) {
			this.helper = helper;
		}

		public SymlinkHelper getHelper() {
			return helper;
		}

		public ClientProcessWrapper getWrapper() {
			return cpw;
		}

		/** Whether this was released; read under this object's monitor. */
		public boolean isReleased() {
			return released;
		}
	}

	/**
	 * The user's shared helper and wrapper on this instance, created on first use. The
	 * chroot folder is per instance, so its name keeps the random suffix.
	 */
	public static Local local(User user) {
		String owner = ownerOf(user);
		Local local = LOCALS.get(owner);
		if (local != null) {
			return local;
		}
		synchronized (LOCALS_LOCK) {
			local = LOCALS.get(owner);
			if (local == null) {
				String chroot = Utility.getDIHelperProperty(Constants.CHROOT_DIR) + "/" + User.getSingleLogginName(user)
						+ "-" + UUID.randomUUID();
				SymlinkHelper helper = new SymlinkHelper(chroot);
				local = new Local(helper);
				// Published before linking the asset folder: opening the asset project can
				// ask the user for this helper again, and must get this one.
				LOCALS.put(owner, local);
				try {
					helper.symlinkUserAsset(user);
				} catch (Exception e) {
					classLogger.warn("Unable to symlink user asset folder into chroot", e);
				}
				ensureSweeper();
			}
			return local;
		}
	}

	// ------------------------------------------------------------------ attach or create

	/** A transport connected to the user's sandbox, and whether that sandbox already existed. */
	public static final class Attached {
		public final OpenShellTransport transport;
		public final int port;
		public final boolean reattached;

		private Attached(OpenShellTransport transport, int port, boolean reattached) {
			this.transport = transport;
			this.port = port;
			this.reattached = reattached;
		}
	}

	/**
	 * Connect to the user's sandbox, creating it if they have none that is running.
	 *
	 * <p>
	 * Creation is serialised per user across instances with a registry lock, so two
	 * instances that both find nothing do not both create; the loser waits for the
	 * winner's entry and attaches to it.
	 */
	public static Attached acquire(User user) throws Exception {
		String owner = ownerOf(user);
		Registry store = registry();
		long deadline = System.currentTimeMillis() + ACQUIRE_TIMEOUT_MS;
		int attachFailures = 0;
		while (System.currentTimeMillis() < deadline) {
			String name = store.sandboxFor(owner);
			if (name != null) {
				String phase;
				try {
					phase = OpenShellTransport.phaseOf(name);
				} catch (IOException e) {
					// Unknown is not gone: deleting here would destroy the user's python over a
					// gateway hiccup.
					classLogger.warn("Could not read the state of sandbox {}; retrying", name, e);
					Thread.sleep(ACQUIRE_POLL_MS);
					continue;
				}
				if ("Ready".equalsIgnoreCase(phase)) {
					OpenShellTransport transport = OpenShellTransport.forUser(name, owner);
					try {
						return new Attached(transport, transport.attach(), true);
					} catch (Exception e) {
						if (++attachFailures < MAX_ATTACH_ATTEMPTS) {
							classLogger.warn("Could not attach to sandbox {}; retrying", name, e);
							Thread.sleep(ACQUIRE_POLL_MS);
							continue;
						}
						classLogger.warn("Could not attach to sandbox {}; replacing it", name, e);
					}
				} else {
					classLogger.info("Sandbox {} for this user is {}; replacing it", name,
							phase == null ? "gone" : phase);
				}
				OpenShellTransport.deleteSandbox(name);
				store.forget(owner, name);
				attachFailures = 0;
				continue;
			}
			String lockToken = store.tryLock(owner);
			if (lockToken != null) {
				try {
					if (store.sandboxFor(owner) != null) {
						continue;
					}
					// No entry, but the gateway may still run the user's sandbox: the in-memory
					// registry after a restart, or Redis flushed. The owner label finds it.
					String labelled = runningSandboxLabelled(owner);
					if (labelled != null) {
						classLogger.info("Recovered sandbox {} for user {} from its owner label", labelled, owner);
						store.bind(owner, labelled);
						continue;
					}
					name = OpenShellTransport.sandboxNameFor(owner + ":" + UUID.randomUUID());
					OpenShellTransport transport = OpenShellTransport.forUser(name, owner);
					int port = transport.start();
					store.bind(owner, name);
					classLogger.info("Created sandbox {} for user {} on {}", name, owner, NODE);
					return new Attached(transport, port, false);
				} finally {
					store.unlock(owner, lockToken);
				}
			}
			// Another instance is creating it; its entry appears when the sandbox is Ready.
			Thread.sleep(ACQUIRE_POLL_MS);
		}
		throw new IllegalStateException("timed out waiting for the user's sandbox");
	}

	/** A {@code Ready} sandbox carrying this owner's label, or null. */
	private static String runningSandboxLabelled(String owner) {
		try {
			for (SandboxInfo box : OpenShellTransport.listExecutorSandboxes()) {
				if (owner.equals(box.owner()) && box.isReady()) {
					return box.name();
				}
			}
		} catch (Exception e) {
			classLogger.warn("Could not list sandboxes to recover one by label", e);
		}
		return null;
	}

	// ------------------------------------------------------------------ session leases

	/**
	 * Record that a session of this user is alive, for as long as the session timeout.
	 * Called on every authenticated request; refreshes at most once a minute per session.
	 *
	 * @param lastTouch  the session's previous touch marker, or null
	 * @param ttlSeconds the session's max inactive interval
	 * @return the marker to store on the session, or null when nothing was refreshed
	 */
	public static String touch(User user, String sessionId, String lastTouch, int ttlSeconds) {
		if (sessionId == null || !appliesTo(user) || !user.isLoggedIn()) {
			return null;
		}
		String owner = ownerOf(user);
		// Every request, not only refreshing ones: this set is what keeps the user's
		// connection here from being released while a session of theirs is still active.
		synchronized (ownerLock(owner)) {
			LOCAL_SESSIONS.computeIfAbsent(owner, k -> ConcurrentHashMap.newKeySet()).add(sessionId);
		}
		long now = System.currentTimeMillis();
		if (lastTouch != null && lastTouch.startsWith(BOOT_ID + ":")) {
			try {
				if (now - Long.parseLong(lastTouch.substring(BOOT_ID.length() + 1)) < TOUCH_INTERVAL_MS) {
					return null;
				}
			} catch (NumberFormatException ignored) {
				// refresh below
			}
		}
		try {
			long ttlMs = ttlSeconds > 0 ? TimeUnit.SECONDS.toMillis(ttlSeconds) : DEFAULT_LEASE_MS;
			registry().touch(owner, sessionId, now + ttlMs);
			ensureSweeper();
		} catch (Exception e) {
			classLogger.warn("Could not refresh the sandbox lease for session {}", sessionId, e);
			return null;
		}
		return BOOT_ID + ":" + now;
	}

	/**
	 * A session left this instance. Releases this instance's connection when it was the
	 * user's last session here.
	 *
	 * <p>
	 * Only a session that is really over -- an explicit logout, or idle past its timeout --
	 * gives up its lease, and the sandbox goes when that was the user's last lease anywhere.
	 * A server stopping also destroys every session (measured 2026-10-23: a Tomcat restart
	 * fired {@code sessionDestroyed} for each one and deleted the sandbox), but the user has
	 * not left: their lease stands, so the restarted or another instance reattaches.
	 *
	 * @param owner the owner recorded on the session ({@link #ownerForSession}); a logout
	 *              may already have dropped the login it was derived from
	 * @param over  true for a logout or a timeout; false when the server is going away
	 */
	public static void sessionEnded(String owner, String sessionId, boolean over) {
		if (owner == null) {
			return;
		}
		synchronized (ownerLock(owner)) {
			Set<String> here = LOCAL_SESSIONS.get(owner);
			if (here != null) {
				here.remove(sessionId);
			}
			if (here == null || here.isEmpty()) {
				LOCAL_SESSIONS.remove(owner);
				releaseLocal(owner);
			}
		}
		if (!over) {
			classLogger.info("Session {} left with the server; keeping its sandbox lease", sessionId);
			return;
		}
		boolean anyLive;
		try {
			anyLive = registry().release(owner, sessionId, System.currentTimeMillis());
		} catch (Exception e) {
			classLogger.warn("Could not release the sandbox lease for session {}; the sweeper will", sessionId, e);
			return;
		}
		if (!anyLive) {
			deleteFor(owner, "last session ended");
		}
	}

	/** Caller holds {@link #ownerLock}. */
	private static void releaseLocal(String owner) {
		Local local = LOCALS.remove(owner);
		if (local == null) {
			return;
		}
		// The same monitor User takes while it binds and starts this wrapper, so a release
		// never runs underneath a request that is connecting.
		synchronized (local) {
			local.released = true;
			try {
				// Write back, close this instance's connection, stop the forward. The sandbox
				// itself is not touched here.
				local.cpw.shutdown(true);
			} catch (Exception e) {
				classLogger.warn("Failed to release the sandbox connection for {}", owner, e);
			}
			local.helper.removeChrootFolder();
		}
	}

	private static Object ownerLock(String owner) {
		return OWNER_LOCKS.computeIfAbsent(owner, k -> new Object());
	}

	private static void deleteFor(String owner, String reason) {
		Registry store = registry();
		String name = store.sandboxFor(owner);
		if (name != null) {
			classLogger.info("Deleting sandbox {} ({})", name, reason);
			OpenShellTransport.deleteSandbox(name);
			store.forget(owner, name);
		}
		store.drop(owner);
	}

	// ------------------------------------------------------------------ sweeper

	private static void ensureSweeper() {
		if (sweeper != null) {
			return;
		}
		synchronized (OpenShellUserSandboxes.class) {
			if (sweeper == null) {
				ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(r -> {
					Thread t = new Thread(r, "openshell-sandbox-sweeper");
					t.setDaemon(true);
					return t;
				});
				service.scheduleWithFixedDelay(OpenShellUserSandboxes::sweepQuietly, 1, SWEEP_INTERVAL_MINUTES,
						TimeUnit.MINUTES);
				sweeper = service;
			}
		}
	}

	private static void sweepQuietly() {
		try {
			sweep();
		} catch (Throwable t) {
			classLogger.warn("Sandbox sweep failed", t);
		}
	}

	/**
	 * Delete what nobody can use any more.
	 *
	 * <ol>
	 * <li>A user with no live session anywhere: their sandbox (a session lost with its
	 * server, so its logout never ran).</li>
	 * <li>A labelled sandbox that is not running (its worker exited after idling, or it
	 * was a session-scoped sandbox orphaned by a restart).</li>
	 * <li>With a shared registry only: a running user sandbox no entry points at.</li>
	 * </ol>
	 */
	public static void sweep() throws Exception {
		Registry store = registry();
		long now = System.currentTimeMillis();
		for (String owner : store.owners()) {
			if (!store.anyLive(owner, now)) {
				synchronized (ownerLock(owner)) {
					LOCAL_SESSIONS.remove(owner);
					releaseLocal(owner);
				}
				deleteFor(owner, "no live session for this user");
			}
		}
		LocalDateTime cutoff = LocalDateTime.now(ZoneOffset.UTC).minus(SWEEP_GRACE);
		int unreadable = 0;
		for (SandboxInfo box : OpenShellTransport.listExecutorSandboxes()) {
			LocalDateTime created;
			try {
				created = LocalDateTime.parse(box.createdAt(), CREATED_AT);
			} catch (DateTimeParseException e) {
				unreadable++;
				continue;
			}
			if (created.isAfter(cutoff)) {
				continue;
			}
			if (!box.isReady()) {
				classLogger.info("Sweeping sandbox {} (phase {})", box.name(), box.phase());
				OpenShellTransport.deleteSandbox(box.name());
				if (box.owner() != null) {
					store.forget(box.owner(), box.name());
				}
			} else if (box.owner() != null && store.shared() && !box.name().equals(store.sandboxFor(box.owner()))) {
				classLogger.info("Sweeping sandbox {} (no registry entry points at it)", box.name());
				OpenShellTransport.deleteSandbox(box.name());
			}
		}
		if (unreadable > 0) {
			// Said out loud: a changed CLI date format would otherwise silently stop all sweeping.
			classLogger.warn("Skipped {} sandbox(es) whose creation time could not be read", unreadable);
		}
	}

	// ------------------------------------------------------------------ registry

	private static Registry registry() {
		Registry current = registry;
		if (current == null) {
			synchronized (OpenShellUserSandboxes.class) {
				if (registry == null) {
					RedisConnectionConfig config = RedisConnectionConfig.fromDIHelper();
					registry = config == null ? new LocalRegistry() : new RedisRegistry(config);
					classLogger.info("OpenShell sandbox registry: {}", registry.shared() ? "redis" : "in-memory");
				}
				current = registry;
			}
		}
		return current;
	}

	/** Owner &rarr; sandbox, owner &rarr; live sessions with expiry, and a create lock. */
	interface Registry {
		boolean shared();

		String sandboxFor(String owner);

		void bind(String owner, String sandboxName);

		/** Remove the entry only if it still names this sandbox. */
		void forget(String owner, String sandboxName);

		/** Take the per-user create lock; the token that holds it, or null when another does. */
		String tryLock(String owner);

		/** Release the create lock, only if this token still holds it. */
		void unlock(String owner, String token);

		void touch(String owner, String sessionId, long expiresAtMs);

		/** Remove a session; true when the user still has a live session. */
		boolean release(String owner, String sessionId, long nowMs);

		boolean anyLive(String owner, long nowMs);

		Set<String> owners();

		/** Forget the user entirely: sessions and index. */
		void drop(String owner);
	}

	/**
	 * The shared registry. Laid out like the room coordination keys
	 * ({@code RoomMessageRedisClient}): every key of one user carries the
	 * {@code {owner}} hash tag, so they share a Redis Cluster slot and a script can
	 * touch them together; the lock is {@code SET NX PX} with a random token, released
	 * only by its holder.
	 */
	static final class RedisRegistry implements Registry {
		private static final String UNLOCK_SCRIPT = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		private static final String FORGET_SCRIPT = "if redis.call('hget', KEYS[1], 'name') == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		/** Forget a user with no session and no sandbox left; 1 when it did. */
		private static final String DROP_SCRIPT = "if redis.call('zcard', KEYS[1]) == 0 and redis.call('exists', KEYS[2]) == 0 then return redis.call('del', KEYS[1]) + 1 else return 0 end";
		/** Every user with an entry or a lease; a separate slot, so kept apart from the scripts. */
		private static final String OWNERS_KEY = "openshell:owners";

		private final RedisConnectionConfig config;

		RedisRegistry(RedisConnectionConfig config) {
			this.config = config;
		}

		@Override
		public boolean shared() {
			return true;
		}

		@Override
		public String sandboxFor(String owner) {
			return withJedis(jedis -> jedis.hget(sandboxKey(owner), "name"));
		}

		@Override
		public void bind(String owner, String sandboxName) {
			Map<String, String> record = new HashMap<>();
			record.put("name", sandboxName);
			record.put("node", NODE);
			record.put("createdAt", String.valueOf(System.currentTimeMillis()));
			record.put("workerPort", String.valueOf(Utility.getDIHelperProperty(OpenShellTransport.WORKER_PORT_PROP)));
			withJedis(jedis -> {
				jedis.hset(sandboxKey(owner), record);
				return jedis.sadd(OWNERS_KEY, owner);
			});
		}

		@Override
		public void forget(String owner, String sandboxName) {
			withJedis(jedis -> jedis.eval(FORGET_SCRIPT, Collections.singletonList(sandboxKey(owner)),
					Collections.singletonList(sandboxName)));
		}

		@Override
		public String tryLock(String owner) {
			String token = UUID.randomUUID().toString();
			boolean held = withJedis(jedis -> "OK"
					.equals(jedis.set(lockKey(owner), token, SetParams.setParams().nx().px(CREATE_LOCK_MS))));
			return held ? token : null;
		}

		@Override
		public void unlock(String owner, String token) {
			withJedis(jedis -> jedis.eval(UNLOCK_SCRIPT, Collections.singletonList(lockKey(owner)),
					Collections.singletonList(token)));
		}

		@Override
		public void touch(String owner, String sessionId, long expiresAtMs) {
			withJedis(jedis -> {
				jedis.zadd(sessionsKey(owner), expiresAtMs, sessionId);
				return jedis.sadd(OWNERS_KEY, owner);
			});
		}

		@Override
		public boolean release(String owner, String sessionId, long nowMs) {
			withJedis(jedis -> jedis.zrem(sessionsKey(owner), sessionId));
			return anyLive(owner, nowMs);
		}

		@Override
		public boolean anyLive(String owner, long nowMs) {
			return withJedis(jedis -> {
				jedis.zremrangeByScore(sessionsKey(owner), Double.NEGATIVE_INFINITY, nowMs);
				return jedis.zcard(sessionsKey(owner)) > 0;
			});
		}

		@Override
		public Set<String> owners() {
			return new HashSet<>(withJedis(jedis -> jedis.smembers(OWNERS_KEY)));
		}

		@Override
		public void drop(String owner) {
			Object dropped = withJedis(jedis -> jedis.eval(DROP_SCRIPT, List.of(sessionsKey(owner), sandboxKey(owner)),
					Collections.emptyList()));
			if (dropped instanceof Long count && count > 0) {
				// A touch racing this re-adds the owner, so the index cannot lose a live user.
				withJedis(jedis -> jedis.srem(OWNERS_KEY, owner));
			}
		}

		private <T> T withJedis(JedisCallback<T> callback) {
			// UnifiedJedis is thread-safe and internally pooled; it is shared and must
			// NOT be closed per call (that would tear down the whole client).
			try {
				return callback.apply(RedisConnectionFactory.getClient(config));
			} catch (Exception e) {
				throw new IllegalStateException("Redis sandbox-registry command failed: " + e.getMessage(), e);
			}
		}

		// The {owner} hash tag co-locates all of a user's keys on a single Redis Cluster
		// hash slot, so multi-key EVAL operations never trigger CROSSSLOT. It is inert on
		// standalone/Sentinel deployments.
		private static String sandboxKey(String owner) {
			return "openshell:{" + owner + "}:sandbox";
		}

		private static String sessionsKey(String owner) {
			return "openshell:{" + owner + "}:sessions";
		}

		private static String lockKey(String owner) {
			return "openshell:{" + owner + "}:lock";
		}

		private interface JedisCallback<T> {
			T apply(UnifiedJedis jedis);
		}
	}

	/** Single-instance fallback: correct only while one server owns every session. */
	static final class LocalRegistry implements Registry {
		private final Map<String, String> sandboxes = new ConcurrentHashMap<>();
		private final Map<String, Map<String, Long>> sessions = new ConcurrentHashMap<>();
		private final Map<String, String> locks = new ConcurrentHashMap<>();

		@Override
		public boolean shared() {
			return false;
		}

		@Override
		public String sandboxFor(String owner) {
			return sandboxes.get(owner);
		}

		@Override
		public void bind(String owner, String sandboxName) {
			sandboxes.put(owner, sandboxName);
		}

		@Override
		public void forget(String owner, String sandboxName) {
			sandboxes.remove(owner, sandboxName);
		}

		@Override
		public String tryLock(String owner) {
			String token = UUID.randomUUID().toString();
			return locks.putIfAbsent(owner, token) == null ? token : null;
		}

		@Override
		public void unlock(String owner, String token) {
			locks.remove(owner, token);
		}

		@Override
		public void touch(String owner, String sessionId, long expiresAtMs) {
			sessions.computeIfAbsent(owner, k -> new ConcurrentHashMap<>()).put(sessionId, expiresAtMs);
		}

		@Override
		public boolean release(String owner, String sessionId, long nowMs) {
			Map<String, Long> live = sessions.get(owner);
			if (live != null) {
				live.remove(sessionId);
			}
			return anyLive(owner, nowMs);
		}

		@Override
		public boolean anyLive(String owner, long nowMs) {
			Map<String, Long> live = sessions.get(owner);
			if (live == null) {
				return false;
			}
			live.values().removeIf(expiry -> expiry <= nowMs);
			return !live.isEmpty();
		}

		@Override
		public Set<String> owners() {
			Set<String> all = new HashSet<>(sandboxes.keySet());
			all.addAll(sessions.keySet());
			return all;
		}

		@Override
		public void drop(String owner) {
			Map<String, Long> live = sessions.get(owner);
			if ((live == null || live.isEmpty()) && !sandboxes.containsKey(owner)) {
				sessions.remove(owner);
			}
		}
	}

	// ------------------------------------------------------------------ helpers

	private static String digest(String value, int hexChars) {
		try {
			byte[] hash = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
			return HexFormat.of().formatHex(hash).substring(0, hexChars);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	private static String nodeName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "unknown";
		}
	}
}
