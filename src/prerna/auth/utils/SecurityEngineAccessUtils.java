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
package prerna.auth.utils;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.util.SystemEngineRegistry;

/**
 * What the engine proxy asks the security database before it lets a call to an
 * engine or project method through: the signed in user's permission, whether
 * they are an admin, whether an engine is public, and the permission an engine
 * gives an app for its custom reactors or another engine for its own code.
 *
 * <p>
 * The proxy asks on every call, so the answers are kept for a short time. They
 * are read again as soon as a permission is saved on this server
 * ({@link SecurityModelUsageLimitUtils#version()}), and no later than
 * {@link #KEPT_FOR} after a change saved on another server of a cluster.
 */
public final class SecurityEngineAccessUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEngineAccessUtils.class);

	private static final Duration KEPT_FOR = Duration.ofSeconds(30);

	private static final Cache<User, UserAnswers> USER_ANSWERS = Caffeine.newBuilder().weakKeys()
			.expireAfterWrite(KEPT_FOR).build();
	private static final Cache<String, Boolean> LISTED = Caffeine.newBuilder().expireAfterWrite(KEPT_FOR)
			.maximumSize(10_000).build();
	private static final Cache<String, Boolean> PUBLIC = Caffeine.newBuilder().expireAfterWrite(KEPT_FOR)
			.maximumSize(10_000).build();
	private static final Object RELOAD_LOCK = new Object();

	private static volatile long answersVersion = Long.MIN_VALUE;
	private static volatile GranteePermissions granteePermissions;

	private SecurityEngineAccessUtils() {
	}

	/**
	 * Whether this JVM holds the security database. The server always does. The
	 * Java worker does not: an engine it holds is a stand-in that sends each call
	 * back to the server, which checks the call there.
	 *
	 * @return whether permissions can be read here
	 */
	public static boolean isSecurityDatabaseLoaded() {
		return SystemEngineRegistry.isSecurityDbLoaded();
	}

	/**
	 * The permission the user holds on an engine or project now: the highest level
	 * among the grants in effect, given to them directly or to a team they are in,
	 * or read-only on a public one.
	 *
	 * @param user    the signed in user
	 * @param id      the engine or project id
	 * @param project true when the id is a project's
	 * @return the level, or null when the user has no access
	 */
	public static AccessPermissionEnum getUserPermission(User user, String id, boolean project) {
		if (user == null || id == null) {
			return null;
		}
		return answersOf(user).levels.computeIfAbsent(key(id, project),
				key -> Optional.ofNullable(project ? SecurityProjectUtils.getCurrentUserProjectLevel(user, id)
						: SecurityEngineUtils.getCurrentUserEngineLevel(user, id)))
				.orElse(null);
	}

	/**
	 * Whether the platform's own code may read an engine or project for a user who
	 * holds no permission on it: a discoverable one, whose details anyone can look
	 * at before asking for access, and a project one of whose insights the user can
	 * open. It never counts for a project's custom reactors.
	 *
	 * @param user    the signed in user
	 * @param id      the engine or project id
	 * @param project true when the id is a project's
	 * @return whether the platform may read it for the user
	 */
	public static boolean isReadableThroughPlatform(User user, String id, boolean project) {
		if (user == null || id == null) {
			return false;
		}
		return answersOf(user).platformReads.computeIfAbsent(key(id, project), key -> {
			if (!project) {
				return SecurityEngineUtils.engineIsDiscoverable(id);
			}
			try {
				return SecurityProjectUtils.projectIsDiscoverable(id)
						|| CurrentPermissionLevel.canOpenAnInsightOf(user, id);
			} catch (IllegalArgumentException e) {
				return false;
			}
		});
	}

	/**
	 * Whether the user is an admin.
	 *
	 * @param user the signed in user
	 * @return true for an admin
	 */
	public static boolean isAdmin(User user) {
		if (user == null) {
			return false;
		}
		UserAnswers answers = answersOf(user);
		Boolean admin = answers.admin;
		if (admin == null) {
			admin = SecurityAdminUtils.storedAsAdmin(user);
			answers.admin = admin;
		}
		return admin;
	}

	/**
	 * Whether the security database lists an engine or project. One that is being
	 * created is not listed yet, and one that is being deleted is no longer.
	 *
	 * @param id      the engine or project id
	 * @param project true when the id is a project's
	 * @return whether it is listed
	 */
	public static boolean isListed(String id, boolean project) {
		if (id == null) {
			return false;
		}
		refreshIfChanged();
		return LISTED.get(key(id, project),
				key -> project ? SecurityProjectUtils.projectExists(id) : SecurityEngineUtils.engineExists(id));
	}

	/**
	 * Whether an engine is public, so everyone may read it.
	 *
	 * @param engineId the engine
	 * @return whether the engine is public
	 */
	public static boolean isPublic(String engineId) {
		if (engineId == null) {
			return false;
		}
		refreshIfChanged();
		return PUBLIC.get(engineId, SecurityEngineUtils::engineIsGlobal);
	}

	/**
	 * The permission an engine gives an app or another engine now, which the app's
	 * custom reactors, or the other engine's own code, use in place of the signed
	 * in user's own. One that has ended does not count.
	 *
	 * @param grantee  the app whose custom code is calling, or the engine whose own
	 *                 code is
	 * @param engineId the engine
	 * @return the level, or null when the engine gives the grantee none
	 */
	public static AccessPermissionEnum getGranteePermission(EngineGrantee grantee, String engineId) {
		return getGranteePermission(grantee, engineId, false);
	}

	/**
	 * The permission an engine or a project gives an app or an engine now, which
	 * the app's custom reactors, its agent runs, or the engine's own code, use in
	 * place of the signed in user's own. One that has ended does not count.
	 *
	 * @param grantee  the app or agent, or the engine whose own code is calling
	 * @param targetId the engine or project
	 * @param project  whether the target is a project
	 * @return the level, or null when the target gives the grantee none
	 */
	public static AccessPermissionEnum getGranteePermission(EngineGrantee grantee, String targetId, boolean project) {
		if (grantee == null || grantee.engineId() == null || grantee.engineType() == null || targetId == null) {
			return null;
		}
		Map<EngineGrantee, AccessPermissionEnum> ofTarget;
		try {
			ofTarget = granteePermissions().byTarget.get(SecurityEngine2EngineUtils.targetKey(targetId, project));
		} catch (IllegalStateException e) {
			classLogger.error(
					"The permissions engines and projects give to apps and engines could not be read, so "
							+ "{} {} is given none on {} {}",
					grantee.engineType(), grantee.engineId(), project ? "project" : "engine", targetId, e);
			return null;
		}
		return ofTarget == null ? null : ofTarget.get(grantee);
	}

	/**
	 * Whether an engine gives any app or other engine a permission now. Most
	 * engines give none, and the engine proxy looks for the app or engine making a
	 * call only on one that does.
	 *
	 * @param engineId the engine
	 * @return whether some app or engine holds a permission on the engine that is
	 *         in effect
	 */
	public static boolean givesAGranteePermission(String engineId) {
		return givesAGranteePermission(engineId, false);
	}

	/**
	 * Whether an engine or a project gives any app or engine a permission now.
	 *
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @return whether some app or engine holds a permission on it that is in effect
	 */
	public static boolean givesAGranteePermission(String targetId, boolean project) {
		if (targetId == null) {
			return false;
		}
		try {
			Map<EngineGrantee, AccessPermissionEnum> ofTarget = granteePermissions().byTarget
					.get(SecurityEngine2EngineUtils.targetKey(targetId, project));
			return ofTarget != null && !ofTarget.isEmpty();
		} catch (IllegalStateException e) {
			classLogger.error("The permissions engines and projects give to apps and engines could not be read, so "
					+ "{} {} is taken to give none", project ? "project" : "engine", targetId, e);
			return false;
		}
	}

	private static String key(String id, boolean project) {
		return (project ? "P:" : "E:") + id;
	}

	private static UserAnswers answersOf(User user) {
		refreshIfChanged();
		return USER_ANSWERS.get(user, key -> new UserAnswers());
	}

	private static void refreshIfChanged() {
		long version = SecurityModelUsageLimitUtils.version();
		if (version != answersVersion) {
			synchronized (RELOAD_LOCK) {
				if (version != answersVersion) {
					USER_ANSWERS.invalidateAll();
					LISTED.invalidateAll();
					PUBLIC.invalidateAll();
					answersVersion = version;
				}
			}
		}
	}

	private static GranteePermissions granteePermissions() {
		long version = SecurityModelUsageLimitUtils.version();
		long now = System.nanoTime();
		GranteePermissions current = granteePermissions;
		if (current != null && current.version == version && now - current.readAt < KEPT_FOR.toNanos()) {
			return current;
		}
		synchronized (RELOAD_LOCK) {
			current = granteePermissions;
			if (current == null || current.version != version || now - current.readAt >= KEPT_FOR.toNanos()) {
				current = new GranteePermissions(version, now, SecurityEngine2EngineUtils.getPermissionsInEffect());
				granteePermissions = current;
			}
			return current;
		}
	}

	/**
	 * The answers kept for one signed in user, until they are read again.
	 */
	private static final class UserAnswers {
		private final Map<String, Optional<AccessPermissionEnum>> levels = new ConcurrentHashMap<>();
		private final Map<String, Boolean> platformReads = new ConcurrentHashMap<>();
		private volatile Boolean admin;
	}

	/**
	 * Every permission an engine or a project gives an app or an engine, as read at
	 * one time, by target ({@link SecurityEngine2EngineUtils#targetKey}).
	 */
	private static final class GranteePermissions {
		private final long version;
		private final long readAt;
		private final Map<String, Map<EngineGrantee, AccessPermissionEnum>> byTarget;

		private GranteePermissions(long version, long readAt,
				Map<String, Map<EngineGrantee, AccessPermissionEnum>> byTarget) {
			this.version = version;
			this.readAt = readAt;
			this.byTarget = byTarget;
		}
	}

}
