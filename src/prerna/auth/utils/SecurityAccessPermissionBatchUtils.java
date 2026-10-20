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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimitPath;
import prerna.auth.access.ModelUsageLimitPath.Reason;
import prerna.auth.access.ModelUsageLimitPath.Step;
import prerna.auth.access.ModelUsageLimitPath.StepLimitType;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.usage.ModelUsagePathView;
import prerna.util.ValueUtils;
import prerna.util.gson.GsonUtility;

/**
 * Bulk Access Management's public methods: read the permissions on a project,
 * an engine or a custom team now, validate batches of changes to them, and save
 * them; and, on a model, read how usage limits apply to one person.
 *
 * <p>
 * Each method uses the signed in user's own permissions. The overloads that
 * take {@link SecurityAdminUtils} are admin mode, and the utilities are the
 * proof: {@link SecurityAdminUtils#getInstance(User)} returns them only for an
 * admin. The validating and saving is done by the package's
 * {@code PermissionBatchUtils}, which nothing outside this package can call.
 * Every change, usage limits included, is validated and saved there, and a
 * grant and its usage limit are written in one transaction.
 * </p>
 *
 * <p>
 * A usage limit path shows how limits apply to one person on a model. Who may
 * read whose path:
 * </p>
 * <ul>
 * <li>Anyone who can use a model reads their own path on it, in full.</li>
 * <li>A model's editors and owners read the path of someone with a grant on it,
 * their own or a team's; the model being public is not enough. The steps that
 * span every model, the User's Total Model Limit and the General Platform
 * Limit, come with {@code rulesHidden} and no rules, and team steps have no
 * team total, since those say what the person may use elsewhere.</li>
 * <li>Admin mode reads anyone's path on any model, in full.</li>
 * </ul>
 */
public final class SecurityAccessPermissionBatchUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityAccessPermissionBatchUtils.class);

	private static final String PATH_EDITORS_ONLY = "Only this model's editors and owners can see how its usage "
			+ "limits apply to someone";
	private static final String PATH_USERS_ONLY = "You do not have access to this model";

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
	 *         access ({@code addsAreAdminOnly}). An engine's grants carry their
	 *         {@code usageLimit}. On a model the result adds {@code publicUse}
	 *         ({@code isPublic} and {@code usageLimit}), {@code modelTotal}
	 *         ({@code usageLimit}), {@code costLimitsAvailable}, {@code price} (the
	 *         price entry its cost is worked out with, or null), and
	 *         {@code allowedPermissionChanges} adds {@code limitLevels} (the levels
	 *         whose usage limits the user may change: all three for owners, none
	 *         for anyone else) and {@code canChangeModelLimits} (whether the user
	 *         may set any usage limit: people's, team limits, the public use limit
	 *         and the total, for owners only)
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
	 * @return one row per person, with {@code subject} (category, id, type, name,
	 *         email), {@code source} (their access on the source, or null for a
	 *         group's members), {@code target} (their access on the target, or
	 *         null) and {@code inTarget}; on an engine, each access carries its
	 *         {@code usageLimit}, so a copy between models can keep it
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
	 * How usage limits apply to one person on a model: every step of "The whole
	 * picture" in order, the ceilings first, with what each rule has used. For the
	 * model's editors and owners, about someone with a grant on it as stored: their
	 * own, or a team's they are in. The model being public is not enough. Teams
	 * from a login provider are known only from the person's own sign-in, so the
	 * stored teams are custom teams.
	 *
	 * <p>
	 * What the person's limits are on other models stays private: the all-models
	 * default and the platform default steps give no {@code rules}, and say
	 * {@code "rulesHidden": true}, and team steps give no {@code teamTotal}
	 * ({@link ModelUsagePathView#toMap(ModelUsageLimitPath, boolean, boolean)}).
	 * </p>
	 *
	 * @param user     an editor or owner of the model
	 * @param engineId the model
	 * @param userId   the person's user id
	 * @param userType the person's login type, in any case
	 * @return the path, as {@link ModelUsagePathView#toMap} writes it with usage
	 * @throws IllegalAccessException   when the user is not an editor or owner of
	 *                                  the model, the same whether or not it exists
	 * @throws IllegalArgumentException when the engine is not a model, the login
	 *                                  type is missing, or the person has no grant
	 *                                  on the model, of their own or a team's
	 */
	public static Map<String, Object> getUsageLimitPath(User user, String engineId, String userId, String userType)
			throws IllegalAccessException {
		String id = ValueUtils.requireNonBlank(engineId, "Must define the model").trim();
		ValueUtils.requireNonBlank(userId, "Must define the user");
		// checked before anything about the model or the person is read, so someone
		// who is not an editor gets the same answer whether or not the model exists
		if (user == null || !SecurityEngineUtils.userCanEditEngine(user, id)) {
			throw new IllegalAccessException(PATH_EDITORS_ONLY);
		}
		// the login type is required: team memberships and a person's limits are
		// kept with it
		String type = SecurityGroupManagerUtils.normalizeUserType(userType);
		requireModel(id);
		ModelUsageLimitPath path = ModelUsageLimitResolver
				.resolve(ModelUsageLimitResolver.storedPerson(userId.trim(), type), id);
		// a public model is open to everyone, so without this an editor of one could
		// read the path of anyone on the platform
		if (!hasGrant(path)) {
			throw new IllegalArgumentException("User " + userId.trim() + " has no access to this model");
		}
		// hides the rules of the steps that span every model, and team totals: this
		// model's editors see only its own limits
		return ModelUsagePathView.toMap(path, true, true);
	}

	/**
	 * {@link #getUsageLimitPath(User, String, String, String)} in admin mode, for
	 * any person on any model, with every step's rules
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 * @throws IllegalArgumentException when the engine does not exist or is not a
	 *                                  model, or the login type is missing
	 */
	public static Map<String, Object> getUsageLimitPath(SecurityAdminUtils adminUtils, User user, String engineId,
			String userId, String userType) {
		requireAdmin(adminUtils);
		String id = ValueUtils.requireNonBlank(engineId, "Must define the model").trim();
		ValueUtils.requireNonBlank(userId, "Must define the user");
		String type = SecurityGroupManagerUtils.normalizeUserType(userType);
		if (!SecurityEngineUtils.engineExists(id)) {
			throw new IllegalArgumentException("Engine " + id + " does not exist");
		}
		requireModel(id);
		// admin mode needs no grant and hides nothing
		return ModelUsagePathView.toMap(
				ModelUsageLimitResolver.resolve(ModelUsageLimitResolver.storedPerson(userId.trim(), type), id), true);
	}

	/**
	 * How usage limits apply to the signed in user on a model, with every login's
	 * grants and teams, those from a login provider included.
	 *
	 * @param user     the signed in user
	 * @param engineId the model
	 * @return the path, as {@link ModelUsagePathView#toMap} writes it with usage
	 * @throws IllegalAccessException   when the user cannot use the model, the same
	 *                                  whether or not it exists
	 * @throws IllegalArgumentException when the engine is not a model, or the user
	 *                                  is anonymous
	 */
	public static Map<String, Object> getMyUsageLimitPath(User user, String engineId) throws IllegalAccessException {
		String id = ValueUtils.requireNonBlank(engineId, "Must define the model").trim();
		if (user == null || !SecurityEngineUtils.userCanViewEngine(user, id)) {
			throw new IllegalAccessException(PATH_USERS_ONLY);
		}
		requireModel(id);
		// the person's own path shows everything, since every limit on it is theirs;
		// personOf reads the signed in logins, so login provider teams count too
		return ModelUsagePathView.toMap(ModelUsageLimitResolver.resolve(ModelUsageLimitResolver.personOf(user), id),
				true);
	}

	/**
	 * How usage limits apply to the signed in user on every model they can use, for
	 * a dashboard of their use: each model by name, with its path. A model whose
	 * limits cannot be read is left out, so it does not hide the others.
	 *
	 * @param user the signed in user
	 * @return {@code models}: each model's {@code engineId}, {@code name},
	 *         {@code subtype} and {@code path}, as {@link ModelUsagePathView#toMap}
	 *         writes it with usage
	 * @throws IllegalAccessException   when there is no user
	 * @throws IllegalArgumentException when the user is anonymous
	 */
	public static Map<String, Object> getMyUsageLimitPaths(User user) throws IllegalAccessException {
		if (user == null) {
			throw new IllegalAccessException(PATH_USERS_ONLY);
		}
		ModelUsageLimitResolver.Person person = ModelUsageLimitResolver.personOf(user);
		// every model the signed in user can use; the user's own paths show everything
		List<Map<String, Object>> engines = SecurityEngineUtils.getUserEngineList(user,
				Collections.singletonList(IEngine.CATALOG_TYPE.MODEL.toString()), null, false, null, null, null, null,
				null);
		Set<String> seen = new HashSet<>();
		List<Map<String, Object>> models = new ArrayList<>();
		for (Map<String, Object> engine : engines) {
			String engineId = ValueUtils.trimToNull(engine.get("engine_id"));
			if (engineId == null || !seen.add(engineId)) {
				continue;
			}
			String name = ValueUtils.trimToNull(engine.get("engine_display_name"));
			if (name == null) {
				name = ValueUtils.trimToNull(engine.get("engine_name"));
			}
			try {
				Map<String, Object> model = new LinkedHashMap<>();
				model.put("engineId", engineId);
				model.put("name", name == null ? engineId : name);
				model.put("subtype", ValueUtils.trimToNull(engine.get("engine_subtype")));
				model.put("path", ModelUsagePathView.toMap(ModelUsageLimitResolver.resolve(person, engineId), true));
				models.add(model);
			} catch (RuntimeException e) {
				// skip a model whose path cannot be read, so it does not hide the others
				classLogger.warn("Unable to read how usage limits apply on model " + engineId, e);
			}
		}
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("models", models);
		return result;
	}

	/**
	 * Checks that the engine is a model before its usage limit path is read.
	 *
	 * @throws IllegalArgumentException when the engine is not a model
	 */
	private static void requireModel(String engineId) {
		if (!PermissionBatchUtils.isModelEngine(engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a model");
		}
	}

	/**
	 * Whether the path has a {@code DIRECT} step that is not skipped for having no
	 * grant, or any {@code TEAM} step, so an editor reads only the paths of people
	 * the model was given to.
	 *
	 * @return whether the path shows the person has a grant on the model as stored:
	 *         their own, or a team's they are in; the model being public is not one
	 */
	private static boolean hasGrant(ModelUsageLimitPath path) {
		// the path always has a DIRECT step, skipped as NO_GRANT when the person has
		// no grant of their own, and a TEAM step only for a team with a grant
		for (Step step : path.getSteps()) {
			if (step.getLimitType() == StepLimitType.DIRECT && step.getReason() != Reason.NO_GRANT) {
				return true;
			}
			if (step.getLimitType() == StepLimitType.TEAM) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Checks that the caller holds the admin utilities, which is what proves admin
	 * mode.
	 *
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
