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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.pipeline.AdminOperation;
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.project.api.IProject;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.SystemEngineRegistry;

/**
 * The check a security utility makes before it adds, changes or deletes an
 * engine, a project or an insight, so a caller that skips its own check is
 * still refused. Each write method names the level it needs, and the signed in
 * user passes when:
 *
 * <ul>
 * <li>they hold that level on the target now, as the permission questions read
 * it ({@link CurrentPermissionLevel});</li>
 * <li>they created the engine or project and no one has been given access to it
 * yet, which covers the steps of creating one before its creator is made its
 * owner, and cleaning one up when creating it fails;</li>
 * <li>they are an admin and the call is part of an admin operation: an admin
 * reactor or endpoint ({@link AdminOperation}).</li>
 * </ul>
 *
 * The platform's own work for no user runs inside a {@link PlatformOperation}
 * and passes. A call with no user outside one is refused and logged, like the
 * engine access check does.
 */
final class ResourceWriteAccess {

	private static final Logger classLogger = LogManager.getLogger(ResourceWriteAccess.class);

	private ResourceWriteAccess() {
	}

	/**
	 * Requires the user to hold a level on an engine.
	 *
	 * @param user     the signed in user, or null for the platform's own work
	 * @param engineId the engine
	 * @param level    the level the change needs
	 * @throws IllegalArgumentException when the user may not make the change
	 */
	static void requireEngine(User user, String engineId, AccessPermissionEnum level) {
		PermissionTargetResource target = PermissionTargetResource.engine(engineId);
		if (isPlatformOrAdmin(user, target) || holds(user, target, level)
				|| isCreatorBeforeAnyGrant(user, target, "ENGINE", "ENGINEID", engineId)) {
			return;
		}
		throw new IllegalArgumentException(refusal("Engine", engineId, level));
	}

	/**
	 * Requires the user to hold a level on a project.
	 *
	 * @param user      the signed in user, or null for the platform's own work
	 * @param projectId the project
	 * @param level     the level the change needs
	 * @throws IllegalArgumentException when the user may not make the change
	 */
	static void requireProject(User user, String projectId, AccessPermissionEnum level) {
		PermissionTargetResource target = PermissionTargetResource.project(projectId);
		if (isPlatformOrAdmin(user, target) || holds(user, target, level)
				|| isCreatorBeforeAnyGrant(user, target, "PROJECT", "PROJECTID", projectId)) {
			return;
		}
		throw new IllegalArgumentException(refusal("Project", projectId, level));
	}

	/**
	 * Requires the user to hold a level on an insight. An owner of the insight's
	 * project holds every level on it.
	 *
	 * @param user      the signed in user, or null for the platform's own work
	 * @param projectId the project that holds the insight
	 * @param insightId the insight
	 * @param level     the level the change needs
	 * @throws IllegalArgumentException when the user may not make the change
	 */
	static void requireInsight(User user, String projectId, String insightId, AccessPermissionEnum level) {
		PermissionTargetResource target = PermissionTargetResource.insight(projectId, insightId);
		if (isPlatformOrAdmin(user, target) || holds(user, target, level)) {
			return;
		}
		throw new IllegalArgumentException(refusal("Insight", insightId, level));
	}

	/**
	 * Requires the user to be the creator of an engine or project that no one has
	 * been given access to yet, so they can be made its owner.
	 *
	 * @param user   the signed in user
	 * @param target the engine or project
	 * @throws IllegalArgumentException when the target already has access given
	 *                                  out, or the user did not create it
	 */
	static void requireNewOwner(User user, PermissionTargetResource target) {
		boolean engine = target.getResourceType() == PermissionTargetResource.TargetResourceType.ENGINE;
		String table = engine ? "ENGINE" : "PROJECT";
		if (isPlatformOrAdmin(user, target)
				|| isCreatorBeforeAnyGrant(user, target, table, table + "ID", target.getId())) {
			return;
		}
		throw new IllegalArgumentException((engine ? "Engine " : "Project ") + target.getId()
				+ " already has owners or was not created by this user");
	}

	/**
	 * Requires the user to be able to add insights to a project, and the insight to
	 * have no access given out yet, so they can be made its owner.
	 *
	 * @param user      the signed in user
	 * @param projectId the project that holds the insight
	 * @param insightId the insight
	 * @throws IllegalArgumentException when the user cannot add insights to the
	 *                                  project or the insight already has access
	 *                                  given out
	 */
	static void requireNewInsightOwner(User user, String projectId, String insightId) {
		PermissionTargetResource target = PermissionTargetResource.insight(projectId, insightId);
		if (isPlatformOrAdmin(user, target)) {
			return;
		}
		requireProject(user, projectId, AccessPermissionEnum.EDIT);
		if (CurrentPermissionLevel.hasAnyGrant(target)) {
			throw new IllegalArgumentException("Insight " + insightId + " already has owners");
		}
	}

	/**
	 * Requires the user to be allowed to create an engine of a type: the publisher
	 * rule, the admin only rule for the type, and for a public engine the admin
	 * only rule for making one public. With no user, the platform registers an
	 * engine from its smss, which gives no one access to it.
	 *
	 * @param user   the signed in user, or null when the platform registers the
	 *               engine
	 * @param type   the engine's type
	 * @param global whether the engine is public
	 * @throws IllegalArgumentException when the user may not create the engine
	 */
	static void requireEngineCreation(User user, IEngine.CATALOG_TYPE type, boolean global) {
		if (user == null || isPlatformOrAdmin(user, null)) {
			return;
		}
		requirePublisher(user);
		boolean admin = SecurityAdminUtils.userIsAdmin(user);
		if (AbstractSecurityUtils.adminOnlyEngineAdd(type) && !admin) {
			throw new IllegalArgumentException("This functionality is limited to only admins");
		}
		if (global && AbstractSecurityUtils.adminOnlyEngineSetPublic(type) && !admin) {
			throw new IllegalArgumentException("Public access can only be enabled by administrators");
		}
	}

	/**
	 * Requires the user to be allowed to create a project of a type: the publisher
	 * rule, the admin only rule for the type, and for a public project the admin
	 * only rule for making one public. With no user, the platform registers a
	 * project from its smss, which gives no one access to it.
	 *
	 * @param user   the signed in user, or null when the platform registers the
	 *               project
	 * @param type   the project's type, or null when it has none
	 * @param global whether the project is public
	 * @throws IllegalArgumentException when the user may not create the project
	 */
	static void requireProjectCreation(User user, IProject.PROJECT_TYPE type, boolean global) {
		if (user == null || isPlatformOrAdmin(user, null)) {
			return;
		}
		requirePublisher(user);
		boolean admin = SecurityAdminUtils.userIsAdmin(user);
		boolean adminOnlyAdd = type == null ? AbstractSecurityUtils.adminOnlyProjectAdd()
				: AbstractSecurityUtils.adminOnlyProjectAdd(type);
		if (adminOnlyAdd && !admin) {
			throw new IllegalArgumentException("This functionality is limited to only admins");
		}
		boolean adminOnlyPublic = type == null ? AbstractSecurityUtils.adminOnlyProjectSetPublic()
				: AbstractSecurityUtils.adminOnlyProjectSetPublic(type);
		if (global && adminOnlyPublic && !admin) {
			throw new IllegalArgumentException("Public access can only be enabled by administrators");
		}
	}

	/**
	 * Requires an admin acting as one, in an admin reactor or endpoint, or the
	 * platform's own work.
	 *
	 * @param user   the signed in user
	 * @param action what the admin is doing, for the message
	 * @throws IllegalArgumentException when the call is not an admin's
	 */
	static void requireAdmin(User user, String action) {
		if (isPlatformOrAdmin(user, null)) {
			return;
		}
		throw new IllegalArgumentException("Only an admin can " + action);
	}

	/**
	 * Requires the platform's own work, inside a {@link PlatformOperation}.
	 *
	 * @param action what the platform is doing, for the message and the log
	 * @throws IllegalArgumentException when no platform operation covers the call
	 */
	static void requirePlatform(String action) {
		if (PlatformOperation.covering() != null) {
			return;
		}
		classLogger.warn("Refused to {}: the call was made outside a platform operation", action);
		throw new IllegalArgumentException("Only the platform can " + action);
	}

	/**
	 * Whether the call is the platform's own work or an admin's, which pass every
	 * check. A call with no user outside a platform operation is logged.
	 */
	private static boolean isPlatformOrAdmin(User user, PermissionTargetResource target) {
		if (PlatformOperation.covering() != null) {
			return true;
		}
		if (user == null) {
			classLogger.warn("Refused a change to {}: no signed in user and outside a platform operation",
					target == null ? "the platform" : target.describe());
			return false;
		}
		return SecurityEngineAccessUtils.isAdmin(user) && AdminOperation.isRunning();
	}

	private static boolean holds(User user, PermissionTargetResource target, AccessPermissionEnum level) {
		return switch (level) {
		case OWNER -> CurrentPermissionLevel.isOwner(user, target);
		case EDIT -> CurrentPermissionLevel.canEdit(user, target);
		case READ_ONLY -> CurrentPermissionLevel.ofOrNull(user, target) != null;
		};
	}

	/**
	 * Whether the user created the engine or project, as its row records, and no
	 * one has been given access to it yet.
	 */
	private static boolean isCreatorBeforeAnyGrant(User user, PermissionTargetResource target, String table,
			String idColumn, String id) {
		if (user == null || id == null) {
			return false;
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table + "__CREATEDBY"));
		qs.addSelector(new QueryColumnSelector(table + "__CREATEDBYTYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__" + idColumn, "==", id));
		qs.setLimit(1);
		Object[] creator;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			if (!wrapper.hasNext()) {
				return false;
			}
			creator = wrapper.next().getValues();
		} catch (Exception e) {
			classLogger.error("Could not read who created {}", target.describe(), e);
			return false;
		}
		return isLoginOf(user, creator[0], creator[1]) && !CurrentPermissionLevel.hasAnyGrant(target);
	}

	/**
	 * Whether a stored id and login type are one of the user's logins. A row with
	 * no login type matches the id alone.
	 */
	private static boolean isLoginOf(User user, Object id, Object type) {
		if (id == null) {
			return false;
		}
		for (AuthProvider login : user.getLogins()) {
			if (!id.toString().equals(user.getAccessToken(login).getId())) {
				continue;
			}
			if (type == null || type.toString().equals(login.getLabel()) || type.toString().equals(login.name())) {
				return true;
			}
		}
		return false;
	}

	private static void requirePublisher(User user) {
		if (AbstractSecurityUtils.adminSetPublisher() && !SecurityQueryUtils.userIsPublisher(user)) {
			throw new IllegalArgumentException("User does not have access to publish databases or projects");
		}
	}

	private static String refusal(String kind, String id, AccessPermissionEnum level) {
		return switch (level) {
		case OWNER -> kind + " " + id + " does not exist or user is not an owner";
		case EDIT -> kind + " " + id + " does not exist or user does not have access to edit";
		case READ_ONLY -> kind + " " + id + " does not exist or user does not have access";
		};
	}
}
