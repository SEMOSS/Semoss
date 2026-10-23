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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
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

	private static final String KEY_PREFIX = "semoss:openshell:";
	/** How often a session may refresh its lease; requests in between are free. */
	private static final long TOUCH_INTERVAL_MS = TimeUnit.SECONDS.toMillis(60);
	/** How long a creator may hold the per-user create lock. */
	private static final long CREATE_LOCK_MS = TimeUnit.MINUTES.toMillis(5);
	/** A labelled sandbox younger than this is never swept: its registry entry may not exist yet. */
	private static final long SWEEP_GRACE_MS = TimeUnit.MINUTES.toMillis(10);
	private static final DateTimeFormatter CREATED_AT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	/** Distinguishes this JVM's leases from ones a restored session carries over a restart. */
	private static final String BOOT_ID = UUID.randomUUID().toString();
	private static final String NODE = nodeName();

	private static final Map<String, Local> LOCALS = new ConcurrentHashMap<>();
	private static final Map<String, Set<String>> LOCAL_SESSIONS = new ConcurrentHashMap<>();
	private static final Object LOCALS_LOCK = new Object();
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
		private final String owner;
		private final SymlinkHelper helper;
		private final ClientProcessWrapper cpw = new ClientProcessWrapper();

		private Local(String owner, SymlinkHelper helper) {
			this.owner = owner;
			this.helper = helper;
		}

		public SymlinkHelper getHelper() {
			return helper;
		}

		public ClientProcessWrapper getWrapper() {
			return cpw;
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
				local = new Local(owner, helper);
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
		long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(240);
		while (System.currentTimeMillis() < deadline) {
			String name = store.sandboxFor(owner);
			if (name != null) {
				String phase = OpenShellTransport.phaseOf(name);
				if ("Ready".equalsIgnoreCase(phase)) {
					OpenShellTransport transport = OpenShellTransport.forUser(name, owner);
					try {
						int port = transport.attach();
						return new Attached(transport, port, true);
					} catch (Exception e) {
						classLogger.warn("Could not attach to sandbox {}; replacing it", name, e);
					}
				} else {
					classLogger.info("Sandbox {} for this user is {}; replacing it", name, phase);
				}
				OpenShellTransport.deleteSandbox(name);
				store.forget(owner, name);
				continue;
			}
			if (store.lock(owner)) {
				try {
					if (store.sandboxFor(owner) != null) {
						continue;
					}
					name = OpenShellTransport.sandboxNameFor(owner + ":" + UUID.randomUUID());
					OpenShellTransport transport = OpenShellTransport.forUser(name, owner);
					int port = transport.start();
					store.bind(owner, name);
					classLogger.info("Created sandbox {} for user {} on {}", name, owner, NODE);
					return new Attached(transport, port, false);
				} finally {
					store.unlock(owner);
				}
			}
			// Another instance is creating it; its entry appears when the sandbox is Ready.
			Thread.sleep(1000);
		}
		throw new IllegalStateException("timed out waiting for the user's sandbox");
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
		if (user == null || sessionId == null || !user.isLoggedIn()) {
			return null;
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
			String owner = ownerOf(user);
			long ttlMs = TimeUnit.SECONDS.toMillis(ttlSeconds > 0 ? ttlSeconds : TimeUnit.DAYS.toSeconds(1));
			registry().touch(owner, sessionId, now + ttlMs);
			LOCAL_SESSIONS.computeIfAbsent(owner, k -> ConcurrentHashMap.newKeySet()).add(sessionId);
			ensureSweeper();
		} catch (Exception e) {
			classLogger.warn("Could not refresh the sandbox lease for session {}", sessionId, e);
			return null;
		}
		return BOOT_ID + ":" + now;
	}

	/**
	 * A session ended (logout or timeout). Releases this instance's connection when it was
	 * the user's last session here, and deletes the sandbox when it was their last anywhere.
	 */
	public static void sessionEnded(User user, String sessionId) {
		if (user == null || !user.isLoggedIn()) {
			return;
		}
		String owner;
		try {
			owner = ownerOf(user);
		} catch (IllegalStateException e) {
			return;
		}
		Set<String> here = LOCAL_SESSIONS.get(owner);
		if (here != null) {
			here.remove(sessionId);
		}
		if (here == null || here.isEmpty()) {
			LOCAL_SESSIONS.remove(owner);
			releaseLocal(owner);
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

	private static void releaseLocal(String owner) {
		Local local = LOCALS.remove(owner);
		if (local == null) {
			return;
		}
		try {
			// Write back, close this instance's connection, stop the forward. The sandbox
			// itself is not touched here.
			local.cpw.shutdown(true);
		} catch (Exception e) {
			classLogger.warn("Failed to release the sandbox connection for {}", owner, e);
		}
		local.helper.removeChrootFolder();
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
				service.scheduleWithFixedDelay(OpenShellUserSandboxes::sweepQuietly, 1, 5, TimeUnit.MINUTES);
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
				Set<String> here = LOCAL_SESSIONS.remove(owner);
				if (here != null) {
					here.clear();
				}
				releaseLocal(owner);
				deleteFor(owner, "no live session for this user");
			}
		}
		LocalDateTime cutoff = LocalDateTime.now(ZoneOffset.UTC).minusNanos(TimeUnit.MILLISECONDS.toNanos(SWEEP_GRACE_MS));
		List<String[]> boxes = OpenShellTransport.listExecutorSandboxes();
		for (String[] box : boxes) {
			String name = box[0];
			String phase = box[1];
			String owner = box[3];
			LocalDateTime created;
			try {
				created = LocalDateTime.parse(box[2], CREATED_AT);
			} catch (Exception e) {
				continue;
			}
			if (created.isAfter(cutoff)) {
				continue;
			}
			boolean running = "Ready".equalsIgnoreCase(phase);
			if (!running) {
				classLogger.info("Sweeping sandbox {} (phase {})", name, phase);
				OpenShellTransport.deleteSandbox(name);
				if (owner != null) {
					store.forget(owner, name);
				}
			} else if (owner != null && store.shared() && !name.equals(store.sandboxFor(owner))) {
				classLogger.info("Sweeping sandbox {} (no registry entry points at it)", name);
				OpenShellTransport.deleteSandbox(name);
			}
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

		boolean lock(String owner);

		void unlock(String owner);

		void touch(String owner, String sessionId, long expiresAtMs);

		/** Remove a session; true when the user still has a live session. */
		boolean release(String owner, String sessionId, long nowMs);

		boolean anyLive(String owner, long nowMs);

		Set<String> owners();

		/** Forget the user entirely: sessions and index. */
		void drop(String owner);
	}

	static final class RedisRegistry implements Registry {
		private final RedisConnectionConfig config;
		private final String lockToken = NODE + ":" + BOOT_ID;

		RedisRegistry(RedisConnectionConfig config) {
			this.config = config;
		}

		private UnifiedJedis redis() {
			return RedisConnectionFactory.getClient(config);
		}

		private static String sandboxKey(String owner) {
			return KEY_PREFIX + "sandbox:" + owner;
		}

		private static String sessionsKey(String owner) {
			return KEY_PREFIX + "sessions:" + owner;
		}

		private static String lockKey(String owner) {
			return KEY_PREFIX + "lock:" + owner;
		}

		private static final String OWNERS = KEY_PREFIX + "owners";

		@Override
		public boolean shared() {
			return true;
		}

		@Override
		public String sandboxFor(String owner) {
			return redis().hget(sandboxKey(owner), "name");
		}

		@Override
		public void bind(String owner, String sandboxName) {
			Map<String, String> record = new HashMap<>();
			record.put("name", sandboxName);
			record.put("node", NODE);
			record.put("createdAt", String.valueOf(System.currentTimeMillis()));
			record.put("workerPort", String.valueOf(Utility.getDIHelperProperty(OpenShellTransport.WORKER_PORT_PROP)));
			UnifiedJedis redis = redis();
			redis.hset(sandboxKey(owner), record);
			redis.sadd(OWNERS, owner);
		}

		@Override
		public void forget(String owner, String sandboxName) {
			UnifiedJedis redis = redis();
			if (sandboxName.equals(redis.hget(sandboxKey(owner), "name"))) {
				redis.del(sandboxKey(owner));
			}
		}

		@Override
		public boolean lock(String owner) {
			return "OK".equals(redis().set(lockKey(owner), lockToken, SetParams.setParams().nx().px(CREATE_LOCK_MS)));
		}

		@Override
		public void unlock(String owner) {
			UnifiedJedis redis = redis();
			if (lockToken.equals(redis.get(lockKey(owner)))) {
				redis.del(lockKey(owner));
			}
		}

		@Override
		public void touch(String owner, String sessionId, long expiresAtMs) {
			UnifiedJedis redis = redis();
			redis.zadd(sessionsKey(owner), expiresAtMs, sessionId);
			redis.sadd(OWNERS, owner);
		}

		@Override
		public boolean release(String owner, String sessionId, long nowMs) {
			redis().zrem(sessionsKey(owner), sessionId);
			return anyLive(owner, nowMs);
		}

		@Override
		public boolean anyLive(String owner, long nowMs) {
			UnifiedJedis redis = redis();
			redis.zremrangeByScore(sessionsKey(owner), Double.NEGATIVE_INFINITY, nowMs);
			return redis.zcard(sessionsKey(owner)) > 0;
		}

		@Override
		public Set<String> owners() {
			return new HashSet<>(redis().smembers(OWNERS));
		}

		@Override
		public void drop(String owner) {
			UnifiedJedis redis = redis();
			if (redis.zcard(sessionsKey(owner)) == 0 && !redis.exists(sandboxKey(owner))) {
				redis.del(sessionsKey(owner));
				redis.srem(OWNERS, owner);
			}
		}
	}

	/** Single-instance fallback: correct only while one server owns every session. */
	static final class LocalRegistry implements Registry {
		private final Map<String, String> sandboxes = new ConcurrentHashMap<>();
		private final Map<String, Map<String, Long>> sessions = new ConcurrentHashMap<>();
		private final Set<String> locks = ConcurrentHashMap.newKeySet();

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
		public boolean lock(String owner) {
			return locks.add(owner);
		}

		@Override
		public void unlock(String owner) {
			locks.remove(owner);
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
			StringBuilder sb = new StringBuilder();
			for (int i = 0; sb.length() < hexChars; i++) {
				sb.append(String.format("%02x", hash[i]));
			}
			return sb.substring(0, hexChars);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	private static String nodeName() {
		try {
			return java.net.InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "unknown";
		}
	}
}
