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
import prerna.util.SystemEngineRegistry;

/**
 * What the engine proxy asks the security database before it lets a call to an
 * engine or project method through: the signed in user's permission, whether
 * they are an admin, and the permission an engine gives a project for its
 * custom reactors.
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
	private static final Object RELOAD_LOCK = new Object();

	private static volatile long answersVersion = Long.MIN_VALUE;
	private static volatile ProjectPermissions projectPermissions;

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
		return answersOf(user).levels
				.computeIfAbsent(key(id, project),
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
			admin = Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user));
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
	 * The permission an engine gives a project now, which the project's custom
	 * reactors use in place of the signed in user's own. One that has ended does
	 * not count.
	 *
	 * @param projectId the project whose custom code is calling
	 * @param engineId  the engine
	 * @return the level, or null when the engine gives the project none
	 */
	public static AccessPermissionEnum getProjectPermission(String projectId, String engineId) {
		if (projectId == null || engineId == null) {
			return null;
		}
		Map<String, AccessPermissionEnum> ofEngine;
		try {
			ofEngine = projectPermissions().byEngine.get(engineId);
		} catch (IllegalStateException e) {
			classLogger.error("The permissions engines give to projects could not be read, so project {} is given "
					+ "none on engine {}", projectId, engineId, e);
			return null;
		}
		return ofEngine == null ? null : ofEngine.get(projectId);
	}

	/**
	 * Whether an engine gives any project a permission now. Most engines give none,
	 * and the engine proxy looks for the app making a call only on one that does.
	 *
	 * @param engineId the engine
	 * @return whether some project holds a permission on the engine that is in
	 *         effect
	 */
	public static boolean givesAProjectPermission(String engineId) {
		if (engineId == null) {
			return false;
		}
		try {
			Map<String, AccessPermissionEnum> ofEngine = projectPermissions().byEngine.get(engineId);
			return ofEngine != null && !ofEngine.isEmpty();
		} catch (IllegalStateException e) {
			classLogger.error("The permissions engines give to projects could not be read, so engine {} is taken "
					+ "to give none", engineId, e);
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
					answersVersion = version;
				}
			}
		}
	}

	private static ProjectPermissions projectPermissions() {
		long version = SecurityModelUsageLimitUtils.version();
		long now = System.nanoTime();
		ProjectPermissions current = projectPermissions;
		if (current != null && current.version == version && now - current.readAt < KEPT_FOR.toNanos()) {
			return current;
		}
		synchronized (RELOAD_LOCK) {
			current = projectPermissions;
			if (current == null || current.version != version || now - current.readAt >= KEPT_FOR.toNanos()) {
				current = new ProjectPermissions(version, now, SecurityProjectEngineUtils.getPermissionsInEffect());
				projectPermissions = current;
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
	 * Every permission an engine gives a project, as read at one time.
	 */
	private static final class ProjectPermissions {
		private final long version;
		private final long readAt;
		private final Map<String, Map<String, AccessPermissionEnum>> byEngine;

		private ProjectPermissions(long version, long readAt,
				Map<String, Map<String, AccessPermissionEnum>> byEngine) {
			this.version = version;
			this.readAt = readAt;
			this.byEngine = byEngine;
		}
	}

}
