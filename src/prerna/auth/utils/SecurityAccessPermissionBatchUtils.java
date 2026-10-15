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

import java.util.List;
import java.util.Map;

import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionTargetResource;
import prerna.util.ValueUtils;
import prerna.util.gson.GsonUtility;

/**
 * Bulk Access Management's public methods: read the permissions on a project,
 * an engine or a custom team now, validate batches of changes to them, and save
 * them.
 *
 * <p>
 * Each method uses the signed in user's own permissions. The overloads that
 * take {@link SecurityAdminUtils} are admin mode, and the utilities are the
 * proof: {@link SecurityAdminUtils#getInstance(User)} returns them only for an
 * admin. The validating and saving is done by the package's
 * {@code PermissionBatchUtils}, which nothing outside this package can call.
 * </p>
 */
public final class SecurityAccessPermissionBatchUtils {

	private SecurityAccessPermissionBatchUtils() {

	}

	/**
	 * Reads batches sent as JSON: a list of
	 * {@code {"target": {...}, "changes": [{"action", "subject", "to"}]}}, with the
	 * field names of {@link PermissionBatch} and the classes it holds.
	 *
	 * @param json the batches
	 * @return the batches
	 * @throws IllegalArgumentException when the text is not a list of batches
	 */
	public static List<PermissionBatch> readBatches(String json) {
		ValueUtils.requireNonBlank(json, "Must define the permission batches");
		List<PermissionBatch> batches;
		try {
			batches = GsonUtility.getDefaultGson().fromJson(json, new TypeToken<List<PermissionBatch>>() {
			}.getType());
		} catch (JsonParseException e) {
			throw new IllegalArgumentException("The permission batches are not valid: " + e.getMessage());
		}
		PermissionBatchUtils.requireBatches(batches);
		return batches;
	}

	/**
	 * Validates batches without saving anything.
	 *
	 * @param user    the user who would save them
	 * @param batches the batches
	 * @return for each batch, its target and each change's index, whether it is
	 *         allowed, and why not when it is not
	 */
	public static List<Map<String, Object>> validateBatches(User user, List<PermissionBatch> batches) {
		return PermissionBatchUtils.validate(user, batches, PermissionBatchOptions.bulkAccessManagement(false));
	}

	/**
	 * {@link #validateBatches(User, List)} in admin mode, where every change is
	 * allowed that the batch's shape and the current access allow.
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 */
	public static List<Map<String, Object>> validateBatches(SecurityAdminUtils adminUtils, User user,
			List<PermissionBatch> batches) {
		requireAdmin(adminUtils);
		return PermissionBatchUtils.validate(user, batches, PermissionBatchOptions.bulkAccessManagement(true));
	}

	/**
	 * Validates batches and saves every change of every batch in one transaction:
	 * either all of them are saved or none is.
	 *
	 * @param user    the user saving them
	 * @param batches the batches
	 * @return how many targets and changes were saved
	 * @throws IllegalArgumentException when any change cannot be made, with the
	 *                                  reasons
	 * @throws IllegalAccessException   when a person picked from the Microsoft
	 *                                  directory cannot be looked up because the
	 *                                  user is not signed in to Microsoft
	 * @throws Exception                when the save fails, after rolling it back
	 */
	public static Map<String, Object> saveBatches(User user, List<PermissionBatch> batches) throws Exception {
		return PermissionBatchUtils.save(user, batches, PermissionBatchOptions.bulkAccessManagement(false));
	}

	/**
	 * {@link #saveBatches(User, List)} in admin mode.
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 */
	public static Map<String, Object> saveBatches(SecurityAdminUtils adminUtils, User user,
			List<PermissionBatch> batches) throws Exception {
		requireAdmin(adminUtils);
		return PermissionBatchUtils.save(user, batches, PermissionBatchOptions.bulkAccessManagement(true));
	}

	/**
	 * Who can use a project or an engine now, or who is in or manages a custom
	 * group, in the shape batches use, and what the user may change. End dates are
	 * ISO dates and times in UTC, so a batch can send them back as they are.
	 *
	 * @param user   the user
	 * @param target the project, engine, custom group's members or custom group's
	 *               managers
	 * @return {@code people}: the people given access directly, or the group's
	 *         members or managers, each with {@code subject} and {@code access}
	 *         (null for a group); {@code teams}: the groups that have access, in
	 *         the same shape, and none for a group;
	 *         {@code allowedPermissionChanges}: whether the user can change
	 *         people's access or the group's members or managers
	 *         ({@code canChangePeople}), give or change Author access
	 *         ({@code canGiveAuthorAccess}), change which groups have access
	 *         ({@code canChangeTeams}), and whether only admins can give people
	 *         access ({@code addsAreAdminOnly})
	 * @throws IllegalAccessException when the user cannot view the target, or
	 *                                cannot manage the group
	 */
	public static Map<String, Object> getPermissions(User user, PermissionTargetResource target)
			throws IllegalAccessException {
		return PermissionBatchUtils.readPermissions(user, target, false);
	}

	/**
	 * {@link #getPermissions(User, PermissionTargetResource)} in admin mode, for
	 * any target
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 */
	public static Map<String, Object> getPermissions(SecurityAdminUtils adminUtils, User user,
			PermissionTargetResource target) throws IllegalAccessException {
		requireAdmin(adminUtils);
		return PermissionBatchUtils.readPermissions(user, target, true);
	}

	/**
	 * The people to copy into a target from a source, with the access each has on
	 * both, so Bulk Access Management can stage additions and show conflicts. From
	 * a project or an engine, the people given access to it directly are copied,
	 * not its groups. From a custom group, its members or its managers are copied;
	 * either can go into a group's members or managers, the same group's included,
	 * so its managers can be copied into its members. Managers are never copied to
	 * or from a project or an engine.
	 *
	 * @param user   the user
	 * @param source a project, engine, custom group's members or custom group's
	 *               managers
	 * @param target the project, engine, custom group's members or custom group's
	 *               managers being changed
	 * @return one row per person, with {@code subject} (kind, id, type, name,
	 *         email), {@code source} (their access on the source, or null for a
	 *         group's members), {@code target} (their access on the target, or
	 *         null) and {@code inTarget}
	 * @throws IllegalAccessException when the user cannot see the source's or the
	 *                                target's people
	 */
	public static List<Map<String, Object>> getPermissionsToCopy(User user, PermissionTargetResource source,
			PermissionTargetResource target) throws IllegalAccessException {
		return PermissionBatchUtils.readPermissionsToCopy(user, source, target, false);
	}

	/**
	 * {@link #getPermissionsToCopy(User, PermissionTargetResource, PermissionTargetResource)}
	 * in admin mode, from and to any project, engine or custom group
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 */
	public static List<Map<String, Object>> getPermissionsToCopy(SecurityAdminUtils adminUtils, User user,
			PermissionTargetResource source, PermissionTargetResource target) throws IllegalAccessException {
		requireAdmin(adminUtils);
		return PermissionBatchUtils.readPermissionsToCopy(user, source, target, true);
	}

	/**
	 * @param adminUtils the admin utilities, from
	 *                   {@link SecurityAdminUtils#getInstance(User)}, which returns
	 *                   null for anyone who is not an admin
	 */
	private static void requireAdmin(SecurityAdminUtils adminUtils) {
		if (adminUtils == null) {
			throw new IllegalArgumentException("Admin mode needs the admin utilities");
		}
	}
}
