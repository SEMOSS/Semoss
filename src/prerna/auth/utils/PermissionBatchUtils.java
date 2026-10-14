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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionBatchException;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionErrorRules;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.date.SemossDate;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.notifications.NotificationDbUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.NotificationConstants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/**
 * Validates and saves permission batches: changes to who can use a project, an
 * engine or an insight, or to who is in or manages a custom group. Every
 * permission change goes through here, and nothing outside this package can
 * call it: Bulk Access Management through
 * {@link SecurityAccessPermissionBatchUtils}, and the single and bulk
 * permission methods, such as {@code SecurityProjectUtils.addProjectUser},
 * through
 * {@link #saveChanges(User, PermissionTargetResource, List, PermissionBatchOptions, PermissionErrorRules)}.
 *
 * <p>
 * A save goes through these steps:
 * </p>
 * <ol>
 * <li>{@code validateBatch} validates each {@link PermissionBatch}: this is
 * where it is decided what can be saved. It makes a
 * {@link PermissionBatchValidation} with a {@link PermissionChangeValidation}
 * for each change, reads the user's {@link PermissionAllowedChanges} on the
 * target once, reads each change, and validates it against the permissions and
 * the current rows. A change, or the whole batch, that cannot be made is
 * blocked with a reason.</li>
 * <li>{@link #save} stops with a {@link PermissionBatchException} holding every
 * {@link PermissionProblem} when anything is blocked, so nothing is saved.
 * {@link #saveChanges} turns those problems into the method's own exception
 * with its {@link PermissionErrorRules}.</li>
 * <li>People picked from the Microsoft directory get an account, and the
 * batches are validated again.</li>
 * <li>{@code writeChange} writes every change of every batch in one
 * transaction, from the values its validation holds.</li>
 * <li>Notifications go out after the save.</li>
 * </ol>
 *
 * <p>
 * Every change follows the same rules: editors change people's access, only
 * owners give or change Author access, only owners change which groups have
 * access, and only a group's managers change its members and its managers.
 * These are the user's own permissions, even when the user is an admin. Admin
 * mode, where every change the batch's shape and the current rows allow is
 * allowed, is on only in {@link PermissionBatchOptions}, which only callers
 * that hold the admin utilities set.
 * </p>
 *
 * <p>
 * A change sets the whole access: its level, its end date, and on an engine its
 * usage limits. Leaving the end date or a limit out clears it.
 * </p>
 */
final class PermissionBatchUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(PermissionBatchUtils.class);

	private static final String INSERT_PROJECT_USER = "INSERT INTO PROJECTPERMISSION (USERID, PROJECTID, PERMISSION, "
			+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES (?,?,?,?,?,?,?)";
	private static final String UPDATE_PROJECT_USER = "UPDATE PROJECTPERMISSION SET PERMISSION=?, "
			+ "PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE USERID=? AND PROJECTID=?";
	private static final String DELETE_PROJECT_USER = "DELETE FROM PROJECTPERMISSION WHERE USERID=? AND PROJECTID=?";
	private static final String DELETE_PROJECT_USER_INSIGHTS = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? "
			+ "AND PROJECTID=?";

	private static final String INSERT_ENGINE_USER = "INSERT INTO ENGINEPERMISSION (USERID, ENGINEID, PERMISSION, "
			+ "PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE, USAGERESTRICTION, USAGEFREQUENCY, "
			+ "MAXTOKENS, MAXRESPONSETIME) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
	private static final String UPDATE_ENGINE_USER = "UPDATE ENGINEPERMISSION SET PERMISSION=?, PERMISSIONGRANTEDBY=?, "
			+ "PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=?, USAGERESTRICTION=?, USAGEFREQUENCY=?, MAXTOKENS=?, "
			+ "MAXRESPONSETIME=? WHERE USERID=? AND ENGINEID=?";
	private static final String UPDATE_ENGINE_USER_KEEPING_LIMITS = "UPDATE ENGINEPERMISSION SET PERMISSION=?, "
			+ "PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE USERID=? AND ENGINEID=?";
	private static final String DELETE_ENGINE_USER = "DELETE FROM ENGINEPERMISSION WHERE USERID=? AND ENGINEID=?";

	private static final String INSERT_INSIGHT_USER = "INSERT INTO USERINSIGHTPERMISSION (USERID, PROJECTID, INSIGHTID, "
			+ "PERMISSION, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE, DATEADDED, ENDDATE) VALUES (?,?,?,?,?,?,?,?)";
	private static final String UPDATE_INSIGHT_USER = "UPDATE USERINSIGHTPERMISSION SET PERMISSION=?, "
			+ "PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=?, DATEADDED=?, ENDDATE=? WHERE USERID=? AND PROJECTID=? "
			+ "AND INSIGHTID=?";
	private static final String DELETE_INSIGHT_USER = "DELETE FROM USERINSIGHTPERMISSION WHERE USERID=? AND PROJECTID=? "
			+ "AND INSIGHTID=?";

	private static final String APPROVE_REQUEST_COLUMNS = " SET PERMISSION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, "
			+ "APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
	private static final String APPROVE_PROJECT_REQUEST = "UPDATE PROJECTACCESSREQUEST" + APPROVE_REQUEST_COLUMNS;
	private static final String APPROVE_ENGINE_REQUEST = "UPDATE ENGINEACCESSREQUEST" + APPROVE_REQUEST_COLUMNS
			+ " AND ENGINEID = ?";
	private static final String APPROVE_INSIGHT_REQUEST = "UPDATE INSIGHTACCESSREQUEST" + APPROVE_REQUEST_COLUMNS;
	private static final String DENY_REQUEST_COLUMNS = " SET APPROVER_USERID = ?, APPROVER_TYPE = ?, "
			+ "APPROVER_DECISION = ?, APPROVER_TIMESTAMP = ? WHERE ID = ?";
	private static final String DENY_PROJECT_REQUEST = "UPDATE PROJECTACCESSREQUEST" + DENY_REQUEST_COLUMNS;
	private static final String DENY_ENGINE_REQUEST = "UPDATE ENGINEACCESSREQUEST" + DENY_REQUEST_COLUMNS
			+ " AND ENGINEID = ?";
	private static final String DENY_INSIGHT_REQUEST = "UPDATE INSIGHTACCESSREQUEST" + DENY_REQUEST_COLUMNS;

	private static final String INSERT_PROJECT_GROUP = "INSERT INTO GROUPPROJECTPERMISSION (ID, TYPE, PROJECTID, "
			+ "PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES (?,?,?,?,?,?,?,?)";
	private static final String UPDATE_PROJECT_GROUP = "UPDATE GROUPPROJECTPERMISSION SET PERMISSION=?, DATEADDED=?, "
			+ "ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND PROJECTID=?";
	private static final String DELETE_PROJECT_GROUP = "DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=? "
			+ "AND PROJECTID=?";

	private static final String INSERT_ENGINE_GROUP = "INSERT INTO GROUPENGINEPERMISSION (ID, TYPE, ENGINEID, "
			+ "PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES (?,?,?,?,?,?,?,?)";
	private static final String UPDATE_ENGINE_GROUP = "UPDATE GROUPENGINEPERMISSION SET PERMISSION=?, DATEADDED=?, "
			+ "ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND ENGINEID=?";
	private static final String DELETE_ENGINE_GROUP = "DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=? "
			+ "AND ENGINEID=?";

	private static final String INSERT_INSIGHT_GROUP = "INSERT INTO GROUPINSIGHTPERMISSION (ID, TYPE, PROJECTID, "
			+ "INSIGHTID, PERMISSION, DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) "
			+ "VALUES (?,?,?,?,?,?,?,?,?)";
	private static final String UPDATE_INSIGHT_GROUP = "UPDATE GROUPINSIGHTPERMISSION SET PERMISSION=?, DATEADDED=?, "
			+ "ENDDATE=?, PERMISSIONGRANTEDBY=?, PERMISSIONGRANTEDBYTYPE=? WHERE ID=? AND TYPE=? AND PROJECTID=? "
			+ "AND INSIGHTID=?";
	private static final String DELETE_INSIGHT_GROUP = "DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=? "
			+ "AND PROJECTID=? AND INSIGHTID=?";

	private static final String INSERT_TEAM_MEMBER = "INSERT INTO CUSTOMGROUPASSIGNMENT (GROUPID, USERID, TYPE, "
			+ "DATEADDED, ENDDATE, PERMISSIONGRANTEDBY, PERMISSIONGRANTEDBYTYPE) VALUES (?,?,?,?,?,?,?)";
	private static final String DELETE_TEAM_MEMBER = "DELETE FROM CUSTOMGROUPASSIGNMENT WHERE GROUPID=? AND USERID=? "
			+ "AND TYPE=?";
	private static final String DELETE_TEAM_MANAGER = "DELETE FROM GROUPMANAGERS WHERE GROUPID=? AND USERID=? AND TYPE=?";

	private PermissionBatchUtils() {

	}

	/*
	 * Checking and saving batches
	 */

	/**
	 * Validates batches without saving anything.
	 *
	 * @return for each batch, its target and each change's index, whether it is
	 *         allowed, and why not when it is not
	 */
	static List<Map<String, Object>> validate(User user, List<PermissionBatch> batches,
			PermissionBatchOptions options) {
		requireBatches(batches);
		List<Map<String, Object>> results = new ArrayList<>();
		for (PermissionBatchValidation validation : validateBatches(user, batches, options)) {
			results.add(validation.toResult());
		}
		return results;
	}

	/**
	 * Checks and saves the access changes one of the single or bulk permission
	 * methods makes, such as {@code addProjectUser}. The options keep what the
	 * method does, such as who it notifies, and the errors keep the exceptions and
	 * messages it throws.
	 *
	 * @param user    the user making the changes
	 * @param batches the changes, grouped by what they change
	 * @param options how the changes are checked and saved
	 * @param errors  the exceptions the method throws for the problems found
	 * @return how many targets and changes were saved
	 * @throws IllegalAccessException when the errors say so, or when a person
	 *                                picked from the Microsoft directory cannot be
	 *                                looked up
	 */
	static Map<String, Object> saveChanges(User user, List<PermissionBatch> batches, PermissionBatchOptions options,
			PermissionErrorRules errors) throws IllegalAccessException {
		try {
			return save(user, batches, options);
		} catch (PermissionBatchException e) {
			throw exceptionFor(errors, e.getProblems(), e);
		} catch (IllegalAccessException | RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Could not save the access changes", e);
			PermissionProblem problem = new PermissionProblem(PermissionProblem.Code.WRITE_FAILED,
					"The access changes could not be saved", batches.get(0).getTarget(), null);
			throw exceptionFor(errors, List.of(problem), new IllegalArgumentException(problem.getMessage(), e));
		}
	}

	/**
	 * {@link #saveChanges(User, List, PermissionBatchOptions, PermissionErrorRules)}
	 * for one target.
	 */
	static Map<String, Object> saveChanges(User user, PermissionTargetResource target, List<PermissionChange> changes,
			PermissionBatchOptions options, PermissionErrorRules errors) throws IllegalAccessException {
		return saveChanges(user, List.of(new PermissionBatch(target, changes)), options, errors);
	}

	/**
	 * {@link #saveChanges(User, List, PermissionBatchOptions, PermissionErrorRules)}
	 * for the methods that do not declare {@link IllegalAccessException}: it is
	 * thrown as an {@link IllegalArgumentException} with the same message.
	 */
	static Map<String, Object> saveChangesNoCheckedException(User user, List<PermissionBatch> batches,
			PermissionBatchOptions options, PermissionErrorRules errors) {
		try {
			return saveChanges(user, batches, options, errors);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	/**
	 * {@link #saveChangesNoCheckedException(User, List, PermissionBatchOptions, PermissionErrorRules)}
	 * for one target.
	 */
	static Map<String, Object> saveChangesNoCheckedException(User user, PermissionTargetResource target,
			List<PermissionChange> changes, PermissionBatchOptions options, PermissionErrorRules errors) {
		return saveChangesNoCheckedException(user, List.of(new PermissionBatch(target, changes)), options, errors);
	}

	/**
	 * @param fallback the batch's own exception, thrown when the errors give none
	 * @return the {@link IllegalAccessException} the errors give for the problems,
	 *         for the caller to throw; an unchecked one is thrown here
	 */
	private static IllegalAccessException exceptionFor(PermissionErrorRules errors, List<PermissionProblem> problems,
			RuntimeException fallback) {
		Exception exception = errors == null ? null : errors.toException(problems);
		if (exception instanceof IllegalAccessException illegalAccess) {
			return illegalAccess;
		}
		if (exception instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw fallback;
	}

	/**
	 * Validates batches and saves every change of every batch in one transaction:
	 * either all of them are saved or none is. It runs six steps, each marked in
	 * the code:
	 * <ol>
	 * <li>Can every change be made ({@link #validateBatches})? If not, nothing is
	 * saved and every problem is reported.</li>
	 * <li>People picked from the Microsoft directory who have no account get
	 * one.</li>
	 * <li>If any did, the batches are checked again.</li>
	 * <li>Who the rows say gave the access, and when.</li>
	 * <li>Every change is saved in one transaction ({@link #writeChange}).</li>
	 * <li>People are notified, and the counts are returned.</li>
	 * </ol>
	 *
	 * @return how many targets and changes were saved
	 * @throws PermissionBatchException when any change cannot be made, with every
	 *                                  problem
	 * @throws IllegalAccessException   when a person picked from the Microsoft
	 *                                  directory cannot be looked up
	 * @throws Exception                when the save fails, after rolling it back
	 */
	static Map<String, Object> save(User user, List<PermissionBatch> batches, PermissionBatchOptions options)
			throws Exception {
		// Step 1: can every change be made? Every batch is validated against the rows
		// as they are now. If any change is refused, nothing is saved, and the
		// caller gets every problem at once so they can fix them all.
		requireBatches(batches);
		List<PermissionBatchValidation> validations = validateBatches(user, batches, options);
		List<PermissionProblem> problems = new ArrayList<>();
		for (PermissionBatchValidation validation : validations) {
			problems.addAll(validation.problems());
		}
		if (!problems.isEmpty()) {
			throw new PermissionBatchException(describeProblems(problems), problems);
		}

		// Step 2: people picked from the Microsoft directory who have no account get
		// one, since a row can only name an account. This is the one change made
		// before the save, and only once step 1 allowed every change. The accounts
		// stay even if the save does not happen.
		boolean addedAccounts = false;
		for (PermissionBatchValidation validation : validations) {
			for (PermissionChangeValidation change : validation.getChanges()) {
				if (change.needsDirectoryAccount()) {
					MicrosoftGraphUserLookup.addMissingUser(user, change.getSubjectId());
					addedAccounts = true;
				}
			}
		}
		// Step 3: if any account was added, the batches are checked again, since step
		// 1 checked them before those accounts existed and someone else may have
		// changed the access meanwhile. A person who still has no account is
		// refused, so no row is saved for them.
		if (addedAccounts) {
			validations = validateBatches(user, batches, options);
			for (PermissionBatchValidation validation : validations) {
				problems.addAll(validation.problems());
				for (PermissionChangeValidation change : validation.getChanges()) {
					if (change.needsDirectoryAccount()) {
						problems.add(new PermissionProblem(PermissionProblem.Code.SUBJECT_MISSING,
								change.getChange().getSubject().describe() + " does not exist", validation.getTarget(),
								change.getIndex(), change.getSubjectId()));
					}
				}
			}
			if (!problems.isEmpty()) {
				throw new PermissionBatchException(describeProblems(problems), problems);
			}
		}
		// the validations of step 1, or of step 3 when it ran, in a variable the save
		// can use
		List<PermissionBatchValidation> toSave = validations;

		// Step 4: who the rows say gave the access, and when. That is the grantor the
		// options name, such as the admin approving a request, or else the user. An
		// admin method that only takes access away may have no user to name.
		Pair<String, String> grantor = options.getGrantor() != null ? options.getGrantor()
				: user == null ? new Pair<>(null, null) : User.getPrimaryUserIdAndTypePair(user);
		Timestamp now = Utility.getCurrentSqlTimestampUTC();

		// Step 5: save. Every change of every batch, and the access requests the
		// batches approve, are written in one transaction, so either all of them are
		// saved or none is. Only connection based helpers run inside, so nothing
		// commits on its own.
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		QueryExecutionUtility.write(securityDb, conn -> {
			for (PermissionBatchValidation validation : toSave) {
				for (PermissionChangeValidation change : validation.getChanges()) {
					writeChange(conn, securityDb, validation.getTarget(), change, grantor, now, options);
				}
			}
			if (options.getApprovedRequests() != null) {
				markApproved(conn, options.getApprovedTarget(), options.getApprovedRequests(), grantor, now);
			}
			return null;
		});

		PermissionBatchAudit.recordChanges(user, toSave, options);

		// Step 6: after the save, the people given access to a project or an engine,
		// or whose access to one changed, are notified as the options say, so nobody
		// hears of access that was not saved. Then the save is logged and its counts
		// returned.
		int changeCount = 0;
		for (PermissionBatchValidation validation : toSave) {
			changeCount += validation.getChanges().size();
			sendNotifications(user, validation, options);
		}
		classLogger.info("User {} applied a permission batch with {} changes to {} targets",
				User.getSingleLogginName(user), changeCount, toSave.size());
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("targets", toSave.size());
		result.put("changes", changeCount);
		return result;
	}

	/**
	 * @return the problems as one message: "Some changes cannot be made: " and the
	 *         first five, each with where it is
	 */
	private static String describeProblems(List<PermissionProblem> problems) {
		List<String> lines = new ArrayList<>();
		for (PermissionProblem problem : problems) {
			String where = problem.getTarget() == null ? "the batch" : problem.getTarget().describe();
			lines.add(where + ": " + problem.getMessage());
		}
		int shown = Math.min(lines.size(), 5);
		String more = lines.size() > shown ? " (and " + (lines.size() - shown) + " more)" : "";
		return "Some changes cannot be made: " + String.join("; ", lines.subList(0, shown)) + more;
	}

	/**
	 * @throws IllegalArgumentException when there are no batches, or one is empty
	 */
	static void requireBatches(List<PermissionBatch> batches) {
		if (batches == null || batches.isEmpty()) {
			throw new IllegalArgumentException("Must define at least one permission batch");
		}
		for (PermissionBatch batch : batches) {
			if (batch == null) {
				throw new IllegalArgumentException("A permission batch is empty");
			}
		}
	}

	/**
	 * @param row a row of the bulk engine methods: {@code permission},
	 *            {@code endDate}, {@code usageRestriction}, {@code usageFrequency},
	 *            {@code maxTokens} and {@code maxResponseTime}, with numbers as
	 *            JSON reads them
	 * @return the access the row gives
	 */
	static PermissionGrant engineGrant(Map<String, ?> row) {
		return new PermissionGrant(ValueUtils.toStringOrNull(row.get("permission")),
				ValueUtils.toStringOrNull(row.get("endDate")))
				.withUsageLimits(ValueUtils.toStringOrNull(row.get("usageRestriction")),
						ValueUtils.toStringOrNull(row.get("usageFrequency")),
						ValueUtils.toNumberOrNull(row.get("maxTokens")),
						ValueUtils.toNumberOrNull(row.get("maxResponseTime")));
	}

	/**
	 * @param rows    the people, each with {@code userid}, {@code permission} and,
	 *                when known, {@code type}
	 * @param endDate when the access ends, the same for every row, or null
	 * @return a change for each row
	 */
	static List<PermissionChange> peopleChanges(PermissionChange.Action action, List<? extends Map<String, ?>> rows,
			String endDate) {
		List<PermissionChange> changes = new ArrayList<>();
		for (Map<String, ?> row : rows) {
			PermissionSubject person = PermissionSubject.ofUser(ValueUtils.toStringOrNull(row.get("userid")),
					ValueUtils.toStringOrNull(row.get("type")));
			changes.add(new PermissionChange(action, person,
					new PermissionGrant(ValueUtils.toStringOrNull(row.get("permission")), endDate)));
		}
		return changes;
	}

	/**
	 * @param rows the people on an engine, each with {@code userid} and the access
	 *             {@link #engineGrant(Map)} reads, and optionally {@code type}
	 * @return a change for each row
	 */
	static List<PermissionChange> engineChanges(PermissionChange.Action action, List<? extends Map<String, ?>> rows) {
		List<PermissionChange> changes = new ArrayList<>();
		for (Map<String, ?> row : rows) {
			PermissionSubject person = PermissionSubject.ofUser(ValueUtils.toStringOrNull(row.get("userid")),
					ValueUtils.toStringOrNull(row.get("type")));
			changes.add(new PermissionChange(action, person, engineGrant(row)));
		}
		return changes;
	}

	/**
	 * The changes that approve access requests: each person gets the level they
	 * asked for, by a change when they have access already and an add when they do
	 * not. {@link PermissionBatchOptions#approving} marks the requests approved in
	 * the same save.
	 *
	 * @param target   the project, engine or insight
	 * @param requests the requests, each with {@code userid} and {@code permission}
	 * @param endDate  when the access ends, or null
	 * @return the changes
	 */
	static List<PermissionChange> approvalChanges(PermissionTargetResource target,
			List<? extends Map<String, ?>> requests, String endDate) {
		List<String> userIds = new ArrayList<>();
		for (Map<String, ?> request : requests) {
			userIds.add(ValueUtils.toStringOrNull(request.get("userid")));
		}
		Map<String, Integer> current = readExistingLevels(target, userIds);
		List<PermissionChange> changes = new ArrayList<>();
		for (Map<String, ?> request : requests) {
			String userId = ValueUtils.toStringOrNull(request.get("userid"));
			PermissionChange.Action action = current.get(userId) != null ? PermissionChange.Action.CHANGE
					: PermissionChange.Action.ADD;
			changes.add(new PermissionChange(action, PermissionSubject.ofUser(userId, null),
					new PermissionGrant(ValueUtils.toStringOrNull(request.get("permission")), endDate)));
		}
		return changes;
	}

	/**
	 * Marks access requests approved, each with the level it asked for.
	 *
	 * @param approver who the rows say gave the access: an id and a login type
	 */
	private static void markApproved(Connection conn, PermissionTargetResource target,
			List<? extends Map<String, ?>> requests, Pair<String, String> approver, Timestamp now) throws Exception {
		boolean isEngine = target.getKind() == PermissionTargetResource.Kind.ENGINE;
		String sql = isEngine ? APPROVE_ENGINE_REQUEST
				: target.getKind() == PermissionTargetResource.Kind.INSIGHT ? APPROVE_INSIGHT_REQUEST
						: APPROVE_PROJECT_REQUEST;
		QueryExecutionUtility.executeBatch(conn, sql, requests, (ps, request) -> {
			int index = 1;
			ps.setInt(index++,
					AccessPermissionEnum.getIdByPermission(ValueUtils.toStringOrNull(request.get("permission"))));
			ps.setString(index++, approver.getValue0());
			ps.setString(index++, approver.getValue1());
			ps.setString(index++, "APPROVED");
			ps.setTimestamp(index++, now);
			ps.setString(index++, ValueUtils.toStringOrNull(request.get("requestid")));
			if (isEngine) {
				ps.setString(index++, target.getId());
			}
		});
	}

	/*
	 * Denying access requests
	 */

	/**
	 * Denies access requests to a project, an engine or an insight: each request is
	 * marked denied, by the user, in one transaction. Who can use the target does
	 * not change, so there is no batch to validate, but the user's permissions are
	 * read as they are for a batch ({@link #readPermissionAllowedChanges}): outside
	 * admin mode, only the target's owners can deny, by their current level, given
	 * to them directly or through a team. A user who is not an editor is blocked as
	 * {@code NO_PERMISSION}, and an editor who is not an owner as
	 * {@code OWNERS_ONLY}.
	 *
	 * @param user       the user denying; in admin mode, null for a method that
	 *                   names who denied them with
	 *                   {@link PermissionBatchOptions#grantedBy}
	 * @param target     the project, engine or insight
	 * @param requestIds the requests' ids
	 * @param options    {@link PermissionBatchOptions#userMode()} or
	 *                   {@link PermissionBatchOptions#adminMode()}
	 * @param errors     the exceptions the method throws for the problems found
	 * @throws IllegalAccessException when the errors say so
	 */
	static void denyRequests(User user, PermissionTargetResource target, List<String> requestIds,
			PermissionBatchOptions options, PermissionErrorRules errors) throws IllegalAccessException {
		String missingField = findMissingTargetField(target);
		if (missingField != null) {
			throw new IllegalArgumentException(missingField);
		}
		PermissionAllowedChanges allowed = readPermissionAllowedChanges(user, target, options.isAdminMode());
		PermissionProblem refused = null;
		if (!allowed.canChangePeople()) {
			refused = new PermissionProblem(allowed.getPeopleCode(), allowed.getPeopleReason(), target, null);
		} else if (!allowed.isOwner()) {
			refused = new PermissionProblem(PermissionProblem.Code.OWNERS_ONLY,
					"Only this " + nounFor(target) + "'s owners can deny access requests", target, null);
		}
		if (refused != null) {
			throw exceptionFor(errors, List.of(refused),
					new PermissionBatchException(refused.getMessage(), List.of(refused)));
		}

		Pair<String, String> denier = options.getGrantor() != null ? options.getGrantor()
				: User.getPrimaryUserIdAndTypePair(user);
		boolean isEngine = target.getKind() == PermissionTargetResource.Kind.ENGINE;
		String sql = isEngine ? DENY_ENGINE_REQUEST
				: target.getKind() == PermissionTargetResource.Kind.INSIGHT ? DENY_INSIGHT_REQUEST
						: DENY_PROJECT_REQUEST;
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		try {
			QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
				QueryExecutionUtility.executeBatch(conn, sql, requestIds, (ps, requestId) -> {
					int index = 1;
					ps.setString(index++, denier.getValue0());
					ps.setString(index++, denier.getValue1());
					ps.setString(index++, "DENIED");
					ps.setTimestamp(index++, now);
					ps.setString(index++, requestId);
					if (isEngine) {
						ps.setString(index++, target.getId());
					}
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Could not deny the access requests to {}", target.describe(), e);
			PermissionProblem failed = new PermissionProblem(PermissionProblem.Code.WRITE_FAILED,
					"The access requests could not be denied", target, null);
			throw exceptionFor(errors, List.of(failed), new IllegalArgumentException(failed.getMessage(), e));
		}
		PermissionBatchAudit.recordDenials(user, target, requestIds, options);
		classLogger.info("User {} denied {} access requests to {}", User.getSingleLogginName(user), requestIds.size(),
				target.describe());
	}

	/**
	 * {@link #denyRequests} for the methods that do not declare
	 * {@link IllegalAccessException}: it is thrown as an
	 * {@link IllegalArgumentException} with the same message.
	 */
	static void denyRequestsNoCheckedException(User user, PermissionTargetResource target, List<String> requestIds,
			PermissionBatchOptions options, PermissionErrorRules errors) {
		try {
			denyRequests(user, target, requestIds, options, errors);
		} catch (IllegalAccessException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	/**
	 * @return the table of the people given access to a project, an engine or an
	 *         insight directly, or null for a team's members or managers
	 */
	private static String permissionTable(PermissionTargetResource target) {
		switch (target.getKind()) {
		case PROJECT:
			return "PROJECTPERMISSION";
		case ENGINE:
			return "ENGINEPERMISSION";
		case INSIGHT:
			return "USERINSIGHTPERMISSION";
		default:
			return null;
		}
	}

	/*
	 * Validating batches: deciding, for each change, whether it can be made
	 */

	/**
	 * Validates every batch. A target can be in only one batch, so all the changes
	 * to it are validated together.
	 *
	 * @return the validation of each batch, in order
	 */
	private static List<PermissionBatchValidation> validateBatches(User user, List<PermissionBatch> batches,
			PermissionBatchOptions options) {
		List<PermissionBatchValidation> validations = new ArrayList<>();
		Set<String> targets = new HashSet<>();
		for (PermissionBatch batch : batches) {
			PermissionBatchValidation validation = validateBatch(user, batch, options);
			if (!validation.isBlocked() && !targets.add(targetKey(validation.getTarget()))) {
				validation.block(PermissionProblem.Code.DUPLICATE,
						"This " + nounFor(validation.getTarget()) + " is in more than one batch");
			}
			validations.add(validation);
		}
		return validations;
	}

	/**
	 * Validates one batch: decides, for each of its changes, whether it can be
	 * saved and, if not, why. Nothing is saved here. It runs six steps, each marked
	 * in the code, and the first three can end the validation early:
	 * <ol>
	 * <li>Is the target fully defined ({@link #findMissingTargetField})?</li>
	 * <li>In admin mode, does the target exist ({@link #targetExists})?</li>
	 * <li>What can the user change on the target
	 * ({@link #readPermissionAllowedChanges})? Someone who can change nothing is
	 * refused on every change ({@link #blockForNoPermission}).</li>
	 * <li>What does each change ask for: who, which level, and until when
	 * ({@link #readChangeFields})? A subject in the batch twice is refused.</li>
	 * <li>What access does everyone in the batch have now
	 * ({@link #readExistingLevels})?</li>
	 * <li>Can each change be made, given the user's permissions and that access?
	 * Each kind of change has its own rules: {@link #validatePersonChange},
	 * {@link #validateTeamChange}, {@link #validateMemberChange} and
	 * {@link #validateManagerChange}.</li>
	 * </ol>
	 *
	 * @return the batch's validation: why the whole batch is blocked, or the result
	 *         of each change
	 */
	private static PermissionBatchValidation validateBatch(User user, PermissionBatch batch,
			PermissionBatchOptions options) {
		PermissionTargetResource target = batch.getTarget();
		PermissionBatchValidation validation = new PermissionBatchValidation(batch);

		// Step 1: is the target fully defined? A project, engine or team needs its
		// id; an insight needs its id and its project's id. This reads only the
		// request.
		String missingField = findMissingTargetField(target);
		if (missingField != null) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED, missingField);
			return validation;
		}

		// Step 2: in admin mode, does the target exist? Admin mode gives every
		// permission
		// without reading the target, so this is where a missing project, engine or
		// insight is caught; a team is read with its permissions in step 3. Outside
		// admin
		// mode a missing target simply gives no permission in step 3, so someone
		// without
		// access cannot tell a missing project or team from one they are not allowed
		// to change, and learns nothing about the people in the batch.
		if (options.isAdminMode() && options.checksTargetExists() && !targetExists(target)) {
			validation.block(PermissionProblem.Code.INVALID_TARGET, notFoundMessage(target));
			return validation;
		}

		// Step 3: what can the user change on the target? Their permissions come from
		// their current level there, given to them directly or through a team, and
		// are read once for the whole batch. Someone who can change nothing is
		// refused on every change before the changes are even read, unless all they
		// do is leave.
		PermissionAllowedChanges allowed;
		try {
			allowed = readPermissionAllowedChanges(user, target, options.isAdminMode());
		} catch (IllegalArgumentException e) {
			// a target whose permissions cannot be read
			validation.block(PermissionProblem.Code.INVALID_TARGET, e.getMessage());
			return validation;
		}
		if (!allowed.canChangePeople() && !allowed.canChangeTeams() && !isOnlyLeaving(user, validation, options)) {
			blockForNoPermission(target, allowed, validation);
			return validation;
		}

		// Step 4: what does each change ask for? Its subject, the level it gives and
		// its end date are read from the request, and a change missing one of them
		// is refused. A subject that is in the batch twice is refused too. The people
		// in the batch are collected for step 5.
		Set<String> seen = new HashSet<>();
		List<String> userIds = new ArrayList<>();
		for (PermissionChangeValidation change : validation.getChanges()) {
			readChangeFields(target, change, options);
			if (change.isBlocked()) {
				continue;
			}
			if (!seen.add(subjectKey(target, change))) {
				change.block(PermissionProblem.Code.DUPLICATE,
						change.getChange().getSubject().describe() + " is in the batch more than once");
				continue;
			}
			if (change.getChange().getSubject().getKind() == PermissionSubject.Kind.USER) {
				userIds.add(change.getSubjectId());
			}
		}

		// Step 5: what access do the people in the batch have now? One query for all
		// of them, so step 6 knows whether each change adds, changes or removes a
		// row that is really there.
		Map<String, Integer> existingLevels = readExistingLevels(target, userIds);

		// Step 6: can each change be made, given the user's permissions from step 3 and
		// the access from step 5? Each kind of change has its own rules: a team's
		// members, a team's managers, a team's access to the target, or a person's
		// access to the target.
		for (PermissionChangeValidation change : validation.getChanges()) {
			if (change.isBlocked()) {
				continue;
			}
			if (target.getKind() == PermissionTargetResource.Kind.TEAM) {
				validateMemberChange(target, allowed, change);
			} else if (target.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS) {
				validateManagerChange(target, allowed, change);
			} else if (change.getChange().getSubject().getKind() == PermissionSubject.Kind.TEAM) {
				validateTeamChange(target, allowed, change, options);
			} else {
				validatePersonChange(user, target, allowed, change, existingLevels, options);
			}
		}
		return validation;
	}

	/**
	 * Blocks the changes of a user who can change nothing on the target, without
	 * reading them, so a malformed change gets the same answer. Each change says
	 * what it needs: giving a team access needs an owner, anything else the
	 * permissions to change people. A batch with no changes is blocked as a whole.
	 */
	private static void blockForNoPermission(PermissionTargetResource target, PermissionAllowedChanges allowed,
			PermissionBatchValidation validation) {
		if (validation.getChanges().isEmpty()) {
			validation.block(allowed.getPeopleCode(), allowed.getPeopleReason());
			return;
		}
		for (PermissionChangeValidation change : validation.getChanges()) {
			PermissionSubject subject = change.getChange() == null ? null : change.getChange().getSubject();
			if (!isTeamKind(target) && subject != null && subject.getKind() == PermissionSubject.Kind.TEAM) {
				change.block(PermissionProblem.Code.NO_PERMISSION, allowed.getTeamsReason());
			} else {
				change.block(allowed.getPeopleCode(), allowed.getPeopleReason());
			}
		}
	}

	/**
	 * @return whether every change takes away the user's own access to a project or
	 *         an engine, which they may do with no other permission when the
	 *         options allow it
	 */
	private static boolean isOnlyLeaving(User user, PermissionBatchValidation validation,
			PermissionBatchOptions options) {
		if (!options.allowsSelfRemoval() || user == null || validation.getChanges().isEmpty()
				|| validation.getTarget().getKind() == PermissionTargetResource.Kind.INSIGHT
				|| isTeamKind(validation.getTarget())) {
			return false;
		}
		String userId = User.getPrimaryUserIdAndTypePair(user).getValue0();
		for (PermissionChangeValidation change : validation.getChanges()) {
			PermissionChange accessChange = change.getChange();
			if (accessChange == null || accessChange.getAction() != PermissionChange.Action.REMOVE
					|| accessChange.getSubject() == null || accessChange.getSubject().getId() == null
					|| !userId.equals(accessChange.getSubject().getId().trim())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Only looks at the target's fields, not at the database.
	 *
	 * @return what the target does not say, such as which project it is, or null
	 *         when it is fully defined
	 */
	private static String findMissingTargetField(PermissionTargetResource target) {
		if (target == null || target.getKind() == null) {
			return "The batch does not say what it changes";
		}
		if (ValueUtils.isBlank(target.getId())) {
			return "The batch does not say which " + nounFor(target) + " it changes";
		}
		if (target.getKind() == PermissionTargetResource.Kind.INSIGHT && ValueUtils.isBlank(target.getProjectId())) {
			return "The batch does not say which project the insight belongs to";
		}
		return null;
	}

	/**
	 * @return whether the project, engine or insight exists; a team is read with
	 *         its permissions instead, so a team target is taken to exist here
	 */
	private static boolean targetExists(PermissionTargetResource target) {
		switch (target.getKind()) {
		case PROJECT:
			return SecurityProjectUtils.projectExists(target.getId());
		case ENGINE:
			return SecurityEngineUtils.engineExists(target.getId());
		case INSIGHT:
			return insightExists(target.getProjectId(), target.getId());
		default:
			return true;
		}
	}

	/**
	 * @return the message for a project, engine or insight that does not exist
	 */
	private static String notFoundMessage(PermissionTargetResource target) {
		switch (target.getKind()) {
		case PROJECT:
			return "Project " + target.getId() + " does not exist";
		case ENGINE:
			return "Engine " + target.getId() + " does not exist";
		case INSIGHT:
			return "Insight " + target.getId() + " does not exist in project " + target.getProjectId();
		default:
			return target.describe() + " does not exist";
		}
	}

	private static boolean insightExists(String projectId, String insightId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__PROJECTID", "==", projectId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("INSIGHT__INSIGHTID", "==", insightId));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			return wrapper.hasNext();
		} catch (Exception e) {
			classLogger.error("Could not check whether insight {} exists", insightId, e);
			return false;
		}
	}

	/**
	 * Reads what the user may do on a target. In admin mode they may do everything;
	 * outside it they have only their own permissions, and being an admin gives
	 * nothing.
	 *
	 * <p>
	 * On a project, an engine or an insight, every permission comes from the user's
	 * highest current level there ({@link CurrentPermissionLevel#of}), whether it
	 * was given to them directly or to a team they are in: editors change people's
	 * access, and owners also give Author access and change which teams have
	 * access. On a custom team, changing its members or its managers needs the user
	 * to manage it.
	 * </p>
	 *
	 * <p>
	 * The permissions are read from the rows as they are before anything is saved,
	 * so the changes in the batches being validated never give the user
	 * permissions, even a change that adds them to a team.
	 * </p>
	 *
	 * @param adminMode whether in admin mode
	 * @return the user's permissions, with the reasons to give when they are
	 *         missing
	 */
	private static PermissionAllowedChanges readPermissionAllowedChanges(User user, PermissionTargetResource target,
			boolean adminMode) {
		PermissionAllowedChanges allowed = new PermissionAllowedChanges();
		String noun = nounFor(target);
		allowed.setPeopleReason("Only this " + noun + "'s editors and owners can change who can use it");
		allowed.setTeamsReason("Only this " + noun + "'s owners can change which teams can use it");
		String id = target.getId();
		switch (target.getKind()) {
		case PROJECT:
		case ENGINE:
		case INSIGHT: {
			Integer level = adminMode ? Integer.valueOf(AccessPermissionEnum.OWNER.getId())
					: CurrentPermissionLevel.of(user, target);
			allowed.setCanChangePeople(level != null && AccessPermissionEnum.isEditor(level));
			allowed.setOwner(level != null && AccessPermissionEnum.isOwner(level));
			allowed.setCanChangeTeams(allowed.isOwner());
			allowed.setAddsAreAdminOnly(!adminMode && allowed.canChangePeople() && isAddAdminOnly(target));
			break;
		}
		case TEAM: {
			boolean isCustom = AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(id);
			allowed.setCanChangePeople(
					isCustom && (adminMode || SecurityGroupManagerUtils.userIsGroupManager(user, id)));
			// only admin mode says a team is not a custom team; anyone else is told
			// only who can change it, so they cannot tell a missing team or a login
			// provider group from a custom team they do not manage
			boolean saysNotCustom = adminMode && !isCustom;
			allowed.setPeopleReason(
					saysNotCustom ? "Team " + id + " is not a custom team, so its members come from the login provider"
							: "Only this team's managers can change its members");
			allowed.setPeopleCode(
					saysNotCustom ? PermissionProblem.Code.INVALID_TARGET : PermissionProblem.Code.NO_PERMISSION);
			break;
		}
		case TEAM_MANAGERS: {
			boolean isCustom = AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(id);
			allowed.setCanChangePeople(
					isCustom && (adminMode || SecurityGroupManagerUtils.userIsGroupManager(user, id)));
			// as for a team's members
			boolean saysNotCustom = adminMode && !isCustom;
			allowed.setPeopleReason(saysNotCustom ? "Team " + id + " is not a custom team, so it has no managers"
					: "Only this team's managers can change who manages it");
			allowed.setPeopleCode(
					saysNotCustom ? PermissionProblem.Code.INVALID_TARGET : PermissionProblem.Code.NO_PERMISSION);
			break;
		}
		}
		return allowed;
	}

	/**
	 * Whether only admins can give people access to the target. An engine type with
	 * no admin-only setting, such as a venv, has no such limit; reading the
	 * engine's setting looks up its type, so it is read only for someone it applies
	 * to.
	 */
	private static boolean isAddAdminOnly(PermissionTargetResource target) {
		switch (target.getKind()) {
		case PROJECT:
			return AbstractSecurityUtils.adminOnlyProjectAddAccess(target.getId());
		case ENGINE:
			try {
				return AbstractSecurityUtils.adminOnlyEngineAddAccess(target.getId());
			} catch (IllegalArgumentException e) {
				return false;
			}
		case INSIGHT:
			return AbstractSecurityUtils.adminOnlyInsightAddAccess();
		default:
			return false;
		}
	}

	/**
	 * Reads a change into its validation: the subject's id and type, the level it
	 * gives and its end date. Blocks it as {@code NOT_WELL_FORMED} when something
	 * it needs is missing, or {@code INVALID_DATE} when the end date cannot be
	 * read. This looks only at the change, not at the database or the user's
	 * permissions.
	 *
	 * <p>
	 * A person given access to a project, an engine or an insight needs no login
	 * type, since those tables keep only the id; a team's members and managers do.
	 * A team's members and managers have no level, so they can only be added or
	 * removed, and managers have no end date.
	 * </p>
	 */
	private static void readChangeFields(PermissionTargetResource target, PermissionChangeValidation validation,
			PermissionBatchOptions options) {
		PermissionChange change = validation.getChange();
		if (change == null || change.getAction() == null) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED, "A change does not say what it does");
			return;
		}
		PermissionSubject subject = change.getSubject();
		if (subject == null || subject.getKind() == null || ValueUtils.isBlank(subject.getId())) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED, "A change does not say who it is for");
			return;
		}
		validation.setSubjectId(subject.getId().trim());
		boolean isTeamTarget = isTeamKind(target);
		boolean isManagers = target.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS;
		if (subject.getKind() == PermissionSubject.Kind.TEAM) {
			if (isTeamTarget) {
				validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
						isManagers ? "A team cannot manage another team" : "A team cannot be a member of another team");
				return;
			}
			if (ValueUtils.isBlank(subject.getType())) {
				validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
						"The change for " + subject.describe() + " does not say what type of team it is");
				return;
			}
			validation.setSubjectType(options.keepsTeamType() ? subject.getType()
					: AuthProvider.getProviderLabel(subject.getType().trim()));
		} else if (!ValueUtils.isBlank(subject.getType())) {
			validation.setSubjectType(SecurityGroupManagerUtils.normalizeUserType(subject.getType()));
		} else if (isTeamTarget) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
					"The change for " + subject.describe() + " does not say how they sign in");
			return;
		}

		if (isTeamTarget && change.getAction() == PermissionChange.Action.CHANGE) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
					isManagers ? "A team's managers have no access level to change"
							: "Team members have no access level to change");
			return;
		}
		boolean needsLevel = !isTeamTarget && change.getAction() != PermissionChange.Action.REMOVE;
		PermissionGrant to = change.getTo();
		if (needsLevel) {
			String level = to == null ? null : to.getLevel();
			validation.setLevelId(
					options.readsLevelsLeniently() ? Integer.valueOf(AccessPermissionEnum.getIdByPermission(level))
							: levelId(level));
			if (validation.getLevelId() == null) {
				validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
						"The change for " + subject.describe() + " does not say which access level to give");
				return;
			}
		}
		if (isManagers && to != null && !ValueUtils.isBlank(to.getEndDate())) {
			validation.block(PermissionProblem.Code.NOT_WELL_FORMED,
					"A team's managers have no end date, so " + subject.describe() + " cannot be given one");
			return;
		}
		if (change.getAction() != PermissionChange.Action.REMOVE && to != null
				&& !ValueUtils.isBlank(to.getEndDate())) {
			try {
				validation.setEndDate(AbstractSecurityUtils.calculateEndDate(to.getEndDate().trim()));
			} catch (DateTimeParseException e) {
				validation.block(PermissionProblem.Code.INVALID_DATE,
						"The end date " + to.getEndDate() + " for " + subject.describe() + " is not a valid date");
			}
		}
	}

	/**
	 * Validates a change to a person's access to a project, an engine or an
	 * insight, and blocks it when it cannot be made:
	 * <ul>
	 * <li>{@code NO_PERMISSION}: the user is not an editor of the target. Anyone
	 * may take away their own access to a project or an engine when the options
	 * allow it.</li>
	 * <li>{@code NO_PERMISSION}: a user who is not an owner gives themselves
	 * access, or changes their own, such as to move or drop its end date. Only
	 * leaving is open to them.</li>
	 * <li>An add: {@code ADMIN_ONLY} when only admins can give access and the
	 * options enforce it; {@code ALREADY_HAS_ACCESS}; {@code GIVES_AUTHOR} when a
	 * non-owner gives Author access; {@code SUBJECT_MISSING} when the person has no
	 * account.</li>
	 * <li>A change: {@code NO_ACCESS} when the person has none to change;
	 * {@code CHANGES_AUTHOR} when a non-owner changes an Author's access;
	 * {@code GIVES_AUTHOR} when a non-owner gives Author access.</li>
	 * <li>A removal: {@code NO_ACCESS} when the person has none to take away;
	 * {@code CHANGES_AUTHOR} when a non-owner takes away an Author's access.</li>
	 * </ul>
	 * A row with no level counts as no access, and an add over it fills it in.
	 * Favorites and visibility used to be kept on the permission row, so saving one
	 * for a person with no grant added a row with no level.
	 *
	 * @param existingLevels each person's level in the rows that exist now, by id;
	 *                       a person whose only rows have no level maps to null
	 */
	private static void validatePersonChange(User user, PermissionTargetResource target,
			PermissionAllowedChanges allowed, PermissionChangeValidation validation,
			Map<String, Integer> existingLevels, PermissionBatchOptions options) {
		PermissionSubject subject = validation.getChange().getSubject();
		PermissionChange.Action action = validation.getChange().getAction();

		// Step 1: may the user change people's access here? They need to be an
		// editor, directly or through a team. Anyone may still leave a project or an
		// engine, whatever their level, when the options allow it.
		boolean isSelfRemoval = options.allowsSelfRemoval() && user != null && action == PermissionChange.Action.REMOVE
				&& target.getKind() != PermissionTargetResource.Kind.INSIGHT
				&& validation.getSubjectId().equals(User.getPrimaryUserIdAndTypePair(user).getValue0());
		if (!allowed.canChangePeople() && !isSelfRemoval) {
			validation.block(PermissionProblem.Code.NO_PERMISSION, allowed.getPeopleReason());
			return;
		}

		// Step 2: is it the user's own access? Someone who is not an owner can only
		// leave: they cannot give themselves access or change their own, such as to
		// move or drop its end date. Otherwise an editor could keep their access past
		// its end, or turn the access a team gives them into a lasting grant of their
		// own.
		boolean isSelf = user != null
				&& AbstractSecurityUtils.getUserFiltersQs(user).contains(validation.getSubjectId());
		if (isSelf && action != PermissionChange.Action.REMOVE && !allowed.isOwner()) {
			validation.block(PermissionProblem.Code.NO_PERMISSION,
					"Only this " + nounFor(target) + "'s owners can change their own access");
			return;
		}

		// Step 3: what access does the person have now? Their level was read with
		// everyone else's in the batch (step 5 of validateBatch). A row with no
		// level counts as no access, and an add fills that row in rather than adding
		// a second one.
		Integer currentLevelId = existingLevels.get(validation.getSubjectId());
		validation.setCurrentLevelId(currentLevelId);
		validation.setHasPreferenceRow(currentLevelId == null && existingLevels.containsKey(validation.getSubjectId()));
		boolean isOwnerNow = currentLevelId != null && AccessPermissionEnum.isOwner(currentLevelId);
		boolean givesOwner = validation.getLevelId() != null && AccessPermissionEnum.isOwner(validation.getLevelId());

		// Step 4: can the change be made, given that access? Only owners give,
		// change or take away Author access.
		switch (action) {
		case ADD:
			// an add needs a target where the user may give access, which some
			// settings keep to admins; someone with no access yet; and an account
			// for them
			if (allowed.addsAreAdminOnly() && options.enforcesAdminOnlyAdds()) {
				validation.block(PermissionProblem.Code.ADMIN_ONLY,
						"Only admins can give people access to this " + nounFor(target));
			} else if (currentLevelId != null) {
				validation.block(PermissionProblem.Code.ALREADY_HAS_ACCESS, subject.describe() + " already has access");
			} else if (givesOwner && !allowed.isOwner()) {
				validation.block(PermissionProblem.Code.GIVES_AUTHOR, "Only owners can give Author access");
			} else {
				validatePersonHasAccount(validation);
			}
			break;
		case CHANGE:
			// a change needs someone who has access to change
			if (currentLevelId == null) {
				validation.block(PermissionProblem.Code.NO_ACCESS,
						subject.describe() + " no longer has access to change");
			} else if (isOwnerNow && !allowed.isOwner()) {
				validation.block(PermissionProblem.Code.CHANGES_AUTHOR, "Only owners can change an Author's access");
			} else if (givesOwner && !allowed.isOwner()) {
				validation.block(PermissionProblem.Code.GIVES_AUTHOR, "Only owners can give Author access");
			}
			break;
		case REMOVE:
			// a removal needs someone who has access to take away; someone leaving
			// may take away their own, whatever it is
			if (currentLevelId == null) {
				validation.block(PermissionProblem.Code.NO_ACCESS,
						subject.describe() + " no longer has access to remove");
			} else if (isOwnerNow && !allowed.isOwner() && !isSelfRemoval) {
				validation.block(PermissionProblem.Code.CHANGES_AUTHOR, "Only owners can remove an Author's access");
			}
			break;
		}
	}

	/**
	 * Validates a change to a team's access to a project, an engine or an insight,
	 * and blocks it when it cannot be made:
	 * <ul>
	 * <li>{@code NO_PERMISSION}: the user is not an owner of the target, directly
	 * or through a team.</li>
	 * <li>An add: {@code SUBJECT_MISSING} when the team is not registered and the
	 * options check that; {@code ALREADY_HAS_ACCESS}.</li>
	 * <li>A change: {@code NO_ACCESS} when the team has none to change;
	 * {@code SAME_LEVEL} when it has the level already and the options refuse
	 * that.</li>
	 * <li>A removal: {@code NO_ACCESS} when the team has none to take away.</li>
	 * </ul>
	 */
	private static void validateTeamChange(PermissionTargetResource target, PermissionAllowedChanges allowed,
			PermissionChangeValidation validation, PermissionBatchOptions options) {
		PermissionSubject subject = validation.getChange().getSubject();

		// Step 1: may the user change which teams have access here? Only owners can,
		// directly or through a team.
		if (!allowed.canChangeTeams()) {
			validation.block(PermissionProblem.Code.NO_PERMISSION, allowed.getTeamsReason());
			return;
		}

		// Step 2: what level does the team have now? It is read from the team's row
		// on the target.
		Integer currentLevelId = readCurrentTeamLevel(target, validation.getSubjectId(), validation.getSubjectType());
		validation.setCurrentLevelId(currentLevelId);

		// Step 3: can the change be made, given that level?
		switch (validation.getChange().getAction()) {
		case ADD:
			// an add needs a team that exists, when the options check that, and that
			// has no access yet
			if (options.checksTeamExists() && !AdminSecurityGroupUtils.getUncheckedInstance()
					.groupExists(validation.getSubjectId(), validation.getSubjectType())) {
				validation.block(PermissionProblem.Code.SUBJECT_MISSING, subject.describe() + " does not exist");
			} else if (currentLevelId != null) {
				validation.block(PermissionProblem.Code.ALREADY_HAS_ACCESS, subject.describe() + " already has access");
			}
			break;
		case CHANGE:
			// a change needs a team that has access to change, and, when the options
			// refuse it, a level other than the one it has
			if (currentLevelId == null) {
				validation.block(PermissionProblem.Code.NO_ACCESS,
						subject.describe() + " no longer has access to change");
			} else if (options.rejectsSameLevel() && currentLevelId.equals(validation.getLevelId())) {
				validation.block(PermissionProblem.Code.SAME_LEVEL, subject.describe() + " already has this level");
			}
			break;
		case REMOVE:
			// a removal needs a team that has access to take away
			if (currentLevelId == null) {
				validation.block(PermissionProblem.Code.NO_ACCESS,
						subject.describe() + " no longer has access to remove");
			}
			break;
		}
	}

	/**
	 * Validates adding or removing a member of a custom team, and blocks it when it
	 * cannot be made:
	 * <ul>
	 * <li>{@code NO_PERMISSION}: the user does not manage the team; or
	 * {@code INVALID_TARGET}: the team is not a custom team, so its members come
	 * from the login provider.</li>
	 * <li>An add: {@code ALREADY_HAS_ACCESS} when the person is a member;
	 * {@code SUBJECT_MISSING} when they have no account.</li>
	 * <li>A removal: {@code NO_ACCESS} when the person is not a member.</li>
	 * </ul>
	 */
	private static void validateMemberChange(PermissionTargetResource target, PermissionAllowedChanges allowed,
			PermissionChangeValidation validation) {
		PermissionSubject subject = validation.getChange().getSubject();

		// Step 1: may the user change this team's members? Only a custom team has
		// members to change, and only its managers change them (any admin, in admin
		// mode). A login provider group's members come from the provider.
		if (!allowed.canChangePeople()) {
			validation.block(allowed.getPeopleCode(), allowed.getPeopleReason());
			return;
		}

		// Step 2: is the person a member now?
		boolean isMember = AdminSecurityGroupUtils.getUncheckedInstance().userInCustomGroup(target.getId(),
				validation.getSubjectId(), validation.getSubjectType());

		// Step 3: can the change be made? An add needs someone who is not a member
		// yet and has an account; a removal needs a member.
		if (validation.getChange().getAction() == PermissionChange.Action.ADD) {
			if (isMember) {
				validation.block(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						subject.describe() + " is already a member");
			} else {
				validatePersonHasAccount(validation);
			}
		} else if (!isMember) {
			validation.block(PermissionProblem.Code.NO_ACCESS, subject.describe() + " is no longer a member to remove");
		}
	}

	/**
	 * Validates adding or removing a manager of a custom team, and blocks it when
	 * it cannot be made:
	 * <ul>
	 * <li>{@code NO_PERMISSION}: the user does not manage the team; or
	 * {@code INVALID_TARGET}: the team is not a custom team, so it has no
	 * managers.</li>
	 * <li>An add: {@code ALREADY_HAS_ACCESS} when the person manages the team;
	 * {@code SUBJECT_MISSING} when they have no account.</li>
	 * <li>A removal: {@code NO_ACCESS} when the person does not manage the
	 * team.</li>
	 * </ul>
	 */
	private static void validateManagerChange(PermissionTargetResource target, PermissionAllowedChanges allowed,
			PermissionChangeValidation validation) {
		PermissionSubject subject = validation.getChange().getSubject();

		// Step 1: may the user change who manages this team? Only a custom team has
		// managers, and only its managers change them (any admin, in admin mode).
		if (!allowed.canChangePeople()) {
			validation.block(allowed.getPeopleCode(), allowed.getPeopleReason());
			return;
		}

		// Step 2: does the person manage the team now?
		boolean isManager = SecurityGroupManagerUtils.managerExists(target.getId(), validation.getSubjectId(),
				validation.getSubjectType());

		// Step 3: can the change be made? An add needs someone who does not manage
		// the team yet and has an account; a removal needs a manager.
		if (validation.getChange().getAction() == PermissionChange.Action.ADD) {
			if (isManager) {
				validation.block(PermissionProblem.Code.ALREADY_HAS_ACCESS,
						subject.describe() + " already manages this team");
			} else {
				validatePersonHasAccount(validation);
			}
		} else if (!isManager) {
			validation.block(PermissionProblem.Code.NO_ACCESS, subject.describe() + " no longer manages this team");
		}
	}

	/**
	 * Blocks an add as {@code SUBJECT_MISSING} when the person has no account. A
	 * person picked from the Microsoft directory gets one when the batch is saved,
	 * so they are marked for it instead. With no login type, any account with the
	 * id will do.
	 */
	private static void validatePersonHasAccount(PermissionChangeValidation validation) {
		String subjectId = validation.getSubjectId();
		String subjectType = validation.getSubjectType();
		boolean hasAccount = subjectType == null ? SecurityQueryUtils.checkUserExist(subjectId)
				: AdminSecurityGroupUtils.getUncheckedInstance().userExists(subjectId, subjectType);
		if (hasAccount) {
			return;
		}
		if (AuthProvider.MICROSOFT.getLabel().equals(subjectType) && MicrosoftGraphUserLookup.isEnabled()) {
			validation.setNeedsDirectoryAccount(true);
			return;
		}
		validation.block(PermissionProblem.Code.SUBJECT_MISSING,
				validation.getChange().getSubject().describe() + " does not exist");
	}

	/*
	 * Current rows
	 */

	/**
	 * Reads the rows that exist now for each person on a project, an engine or an
	 * insight, in one query, ended or not, since a change writes over every row a
	 * person has. A person with more than one row gets the highest level among
	 * them; a person whose only rows have no level maps to null.
	 *
	 * @return each person's level in their rows, by id; a person with no row is not
	 *         in the map
	 * @throws IllegalArgumentException when the rows cannot be read
	 */
	private static Map<String, Integer> readExistingLevels(PermissionTargetResource target, List<String> userIds) {
		Map<String, Integer> levels = new HashMap<>();
		String table = permissionTable(target);
		if (userIds.isEmpty() || table == null) {
			return levels;
		}
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table + "__USERID"));
		qs.addSelector(new QueryColumnSelector(table + "__PERMISSION"));
		CurrentPermissionLevel.filterToTarget(qs, table, target);
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__USERID", "==", userIds));
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance()
				.getRawWrapper(SystemEngineRegistry.getSecurityDb(), qs)) {
			while (wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String userId = String.valueOf(row[0]);
				Integer level = row[1] instanceof Number number ? Integer.valueOf(number.intValue()) : null;
				Integer best = levels.get(userId);
				if (!levels.containsKey(userId) || (level != null && (best == null || level < best))) {
					levels.put(userId, level);
				}
			}
		} catch (Exception e) {
			classLogger.error("Could not read who has access to {}", target.describe(), e);
			throw new IllegalArgumentException("Could not read who has access to " + target.describe());
		}
		return levels;
	}

	/**
	 * @return a team's current level on a project, an engine or an insight, or null
	 *         for none
	 */
	private static Integer readCurrentTeamLevel(PermissionTargetResource target, String groupId, String groupType) {
		switch (target.getKind()) {
		case PROJECT:
			return SecurityGroupProjectUtils.getGroupProjectPermission(groupId, groupType, target.getId());
		case ENGINE:
			return SecurityGroupEngineUtils.getGroupDatabasePermission(groupId, groupType, target.getId());
		case INSIGHT:
			return SecurityGroupInsightsUtils.getGroupInsightPermission(groupId, groupType, target.getProjectId(),
					target.getId());
		default:
			return null;
		}
	}

	/*
	 * Saving
	 */

	private static void writeChange(Connection conn, IRDBMSEngine securityDb, PermissionTargetResource target,
			PermissionChangeValidation validation, Pair<String, String> grantor, Timestamp now,
			PermissionBatchOptions options) throws Exception {
		// an add over a row with no level fills in that row rather than adding a
		// second one, since the tables have no key
		PermissionChange.Action action = validation.getChange().getAction() == PermissionChange.Action.ADD
				&& validation.hasPreferenceRow() ? PermissionChange.Action.CHANGE : validation.getChange().getAction();
		boolean isTeam = validation.getChange().getSubject().getKind() == PermissionSubject.Kind.TEAM;
		String id = target.getId();
		String projectId = target.getProjectId();
		String subjectId = validation.getSubjectId();
		String subjectType = validation.getSubjectType();
		int level = validation.getLevelId() == null ? AccessPermissionEnum.READ_ONLY.getId() : validation.getLevelId();
		Timestamp endDate = validation.getEndDate();
		PermissionGrant to = validation.getChange().getTo();

		if (target.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS) {
			if (action == PermissionChange.Action.ADD) {
				QueryExecutionUtility.executeUpdate(conn, SecurityGroupManagerUtils.INSERT_MANAGER_QUERY, ps -> {
					int i = 1;
					ps.setString(i++, id);
					ps.setString(i++, subjectId);
					ps.setString(i++, subjectType);
					ps.setTimestamp(i++, now);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
				});
			} else {
				QueryExecutionUtility.executeUpdate(conn, DELETE_TEAM_MANAGER, ps -> {
					int i = 1;
					ps.setString(i++, id);
					ps.setString(i++, subjectId);
					ps.setString(i++, subjectType);
				});
			}
			return;
		}

		if (target.getKind() == PermissionTargetResource.Kind.TEAM) {
			if (action == PermissionChange.Action.ADD) {
				QueryExecutionUtility.executeUpdate(conn, INSERT_TEAM_MEMBER, ps -> {
					int i = 1;
					ps.setString(i++, id);
					ps.setString(i++, subjectId);
					ps.setString(i++, subjectType);
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
				});
			} else {
				QueryExecutionUtility.executeUpdate(conn, DELETE_TEAM_MEMBER, ps -> {
					int i = 1;
					ps.setString(i++, id);
					ps.setString(i++, subjectId);
					ps.setString(i++, subjectType);
				});
			}
			return;
		}

		if (isTeam) {
			writeTeamGrant(conn, target, action, subjectId, subjectType, level, endDate, grantor, now);
			return;
		}

		switch (target.getKind()) {
		case PROJECT:
			if (action == PermissionChange.Action.ADD) {
				QueryExecutionUtility.executeUpdate(conn, INSERT_PROJECT_USER, ps -> {
					int i = 1;
					ps.setString(i++, subjectId);
					ps.setString(i++, id);
					ps.setInt(i++, level);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
				});
			} else if (action == PermissionChange.Action.CHANGE) {
				QueryExecutionUtility.executeUpdate(conn, UPDATE_PROJECT_USER, ps -> {
					int i = 1;
					ps.setInt(i++, level);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
					ps.setString(i++, subjectId);
					ps.setString(i++, id);
				});
			} else {
				QueryExecutionUtility.executeUpdate(conn, DELETE_PROJECT_USER, ps -> {
					ps.setString(1, subjectId);
					ps.setString(2, id);
				});
				if (options.removesInsightPermissions()) {
					QueryExecutionUtility.executeUpdate(conn, DELETE_PROJECT_USER_INSIGHTS, ps -> {
						ps.setString(1, subjectId);
						ps.setString(2, id);
					});
				}
			}
			break;
		case ENGINE:
			if (action == PermissionChange.Action.ADD) {
				QueryExecutionUtility.executeUpdate(conn, INSERT_ENGINE_USER, ps -> {
					int i = 1;
					ps.setString(i++, subjectId);
					ps.setString(i++, id);
					ps.setInt(i++, level);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
					setModelLimits(securityDb, ps, i, to, options.storesZeroLimitsAsNone());
				});
			} else if (action == PermissionChange.Action.CHANGE) {
				boolean keepLimits = options.keepsModelLimits() && !validation.hasPreferenceRow();
				QueryExecutionUtility.executeUpdate(conn,
						keepLimits ? UPDATE_ENGINE_USER_KEEPING_LIMITS : UPDATE_ENGINE_USER, ps -> {
							int i = 1;
							ps.setInt(i++, level);
							ps.setString(i++, grantor.getValue0());
							ps.setString(i++, grantor.getValue1());
							ps.setTimestamp(i++, now);
							setNullableTimestamp(ps, i++, endDate);
							if (!keepLimits) {
								i = setModelLimits(securityDb, ps, i, to, options.storesZeroLimitsAsNone());
							}
							ps.setString(i++, subjectId);
							ps.setString(i++, id);
						});
			} else {
				QueryExecutionUtility.executeUpdate(conn, DELETE_ENGINE_USER, ps -> {
					ps.setString(1, subjectId);
					ps.setString(2, id);
				});
			}
			break;
		case INSIGHT:
			if (action == PermissionChange.Action.ADD) {
				QueryExecutionUtility.executeUpdate(conn, INSERT_INSIGHT_USER, ps -> {
					int i = 1;
					ps.setString(i++, subjectId);
					ps.setString(i++, projectId);
					ps.setString(i++, id);
					ps.setInt(i++, level);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
				});
			} else if (action == PermissionChange.Action.CHANGE) {
				QueryExecutionUtility.executeUpdate(conn, UPDATE_INSIGHT_USER, ps -> {
					int i = 1;
					ps.setInt(i++, level);
					ps.setString(i++, grantor.getValue0());
					ps.setString(i++, grantor.getValue1());
					ps.setTimestamp(i++, now);
					setNullableTimestamp(ps, i++, endDate);
					ps.setString(i++, subjectId);
					ps.setString(i++, projectId);
					ps.setString(i++, id);
				});
			} else {
				QueryExecutionUtility.executeUpdate(conn, DELETE_INSIGHT_USER, ps -> {
					ps.setString(1, subjectId);
					ps.setString(2, projectId);
					ps.setString(3, id);
				});
			}
			break;
		default:
			break;
		}
	}

	private static void writeTeamGrant(Connection conn, PermissionTargetResource target, PermissionChange.Action action,
			String groupId, String groupType, int level, Timestamp endDate, Pair<String, String> grantor, Timestamp now)
			throws Exception {
		boolean isInsight = target.getKind() == PermissionTargetResource.Kind.INSIGHT;
		String insert = target.getKind() == PermissionTargetResource.Kind.PROJECT ? INSERT_PROJECT_GROUP
				: target.getKind() == PermissionTargetResource.Kind.ENGINE ? INSERT_ENGINE_GROUP : INSERT_INSIGHT_GROUP;
		String update = target.getKind() == PermissionTargetResource.Kind.PROJECT ? UPDATE_PROJECT_GROUP
				: target.getKind() == PermissionTargetResource.Kind.ENGINE ? UPDATE_ENGINE_GROUP : UPDATE_INSIGHT_GROUP;
		String delete = target.getKind() == PermissionTargetResource.Kind.PROJECT ? DELETE_PROJECT_GROUP
				: target.getKind() == PermissionTargetResource.Kind.ENGINE ? DELETE_ENGINE_GROUP : DELETE_INSIGHT_GROUP;
		String id = target.getId();
		String projectId = target.getProjectId();
		if (action == PermissionChange.Action.ADD) {
			QueryExecutionUtility.executeUpdate(conn, insert, ps -> {
				int i = 1;
				ps.setString(i++, groupId);
				ps.setString(i++, groupType);
				if (isInsight) {
					ps.setString(i++, projectId);
				}
				ps.setString(i++, id);
				ps.setInt(i++, level);
				ps.setTimestamp(i++, now);
				setNullableTimestamp(ps, i++, endDate);
				ps.setString(i++, grantor.getValue0());
				ps.setString(i++, grantor.getValue1());
			});
		} else if (action == PermissionChange.Action.CHANGE) {
			QueryExecutionUtility.executeUpdate(conn, update, ps -> {
				int i = 1;
				ps.setInt(i++, level);
				ps.setTimestamp(i++, now);
				setNullableTimestamp(ps, i++, endDate);
				ps.setString(i++, grantor.getValue0());
				ps.setString(i++, grantor.getValue1());
				ps.setString(i++, groupId);
				ps.setString(i++, groupType);
				if (isInsight) {
					ps.setString(i++, projectId);
				}
				ps.setString(i++, id);
			});
		} else {
			QueryExecutionUtility.executeUpdate(conn, delete, ps -> {
				int i = 1;
				ps.setString(i++, groupId);
				ps.setString(i++, groupType);
				if (isInsight) {
					ps.setString(i++, projectId);
				}
				ps.setString(i++, id);
			});
		}
	}

	private static void setNullableTimestamp(PreparedStatement ps, int index, Timestamp value) throws SQLException {
		if (value == null) {
			ps.setNull(index, Types.TIMESTAMP);
		} else {
			ps.setTimestamp(index, value);
		}
	}

	/**
	 * Binds a model engine's usage limits, as the bulk engine methods do: blank
	 * text and missing numbers are stored as null, and so are limits of 0 when
	 * asked, as the single engine methods store them.
	 *
	 * @return the next parameter index
	 */
	private static int setModelLimits(IRDBMSEngine securityDb, PreparedStatement ps, int index, PermissionGrant to,
			boolean zeroIsNone) throws SQLException {
		int i = index;
		securityDb.getQueryUtil().setStringEmptyAsNullable(ps, i++,
				to == null ? null : ValueUtils.trimToNull(to.getUsageRestriction()));
		securityDb.getQueryUtil().setStringEmptyAsNullable(ps, i++,
				to == null ? null : ValueUtils.trimToNull(to.getUsageFrequency()));
		Integer maxTokens = to == null ? null : to.getMaxTokens();
		if (maxTokens == null || (zeroIsNone && maxTokens == 0)) {
			ps.setNull(i++, Types.INTEGER);
		} else {
			ps.setInt(i++, maxTokens);
		}
		Double maxResponseTime = to == null ? null : to.getMaxResponseTime();
		if (maxResponseTime == null || (zeroIsNone && maxResponseTime == 0.0)) {
			ps.setNull(i++, Types.DOUBLE);
		} else {
			ps.setDouble(i++, maxResponseTime);
		}
		return i;
	}

	/**
	 * Sends the notifications the bulk project and engine methods send for added
	 * and changed people, as the options ask. Notifications are kept in their own
	 * database, so a failure is logged and does not undo the save.
	 */
	private static void sendNotifications(User user, PermissionBatchValidation validation,
			PermissionBatchOptions options) {
		PermissionTargetResource target = validation.getTarget();
		boolean isProject = target.getKind() == PermissionTargetResource.Kind.PROJECT;
		boolean isEngine = target.getKind() == PermissionTargetResource.Kind.ENGINE;
		if ((!isProject && !isEngine) || !Utility.isNotificationDatabaseEnabled()) {
			return;
		}
		String source;
		try {
			source = isProject ? NotificationConstants.APP_CATALOG
					: String.valueOf(SecurityEngineUtils.getEngineType(target.getId())).toLowerCase(Locale.ROOT);
		} catch (Exception e) {
			classLogger.warn("Could not read the type of engine {} for notifications", target.getId(), e);
			return;
		}
		for (PermissionChangeValidation change : validation.getChanges()) {
			if (change.getChange().getSubject().getKind() != PermissionSubject.Kind.USER
					|| change.getChange().getAction() == PermissionChange.Action.REMOVE) {
				continue;
			}
			boolean isAdd = change.getChange().getAction() == PermissionChange.Action.ADD;
			if (isAdd ? !options.notifiesAdds() : !options.notifiesChanges()) {
				continue;
			}
			String previous = isAdd ? null : AccessPermissionEnum.getPermissionValueById(change.getCurrentLevelId());
			try {
				NotificationDbUtils.createNotification(user, change.getSubjectId(), change.getSubjectType(),
						target.getId(),
						isAdd ? NotificationConstants.Type.USER_ADDITION : NotificationConstants.Type.PERMISSION_CHANGE,
						source, NotificationConstants.Priority.MEDIUM, previous,
						AccessPermissionEnum.getPermissionValueById(change.getLevelId()),
						NotificationConstants.DisplaySurface.BELL);
			} catch (Exception e) {
				classLogger.warn("Could not send the access notification for {} on {}", change.getSubjectId(),
						target.describe(), e);
			}
		}
	}

	/*
	 * Current access
	 */

	/**
	 * @see SecurityAccessPermissionBatchUtils#getPermissions(User,
	 *      PermissionTargetResource)
	 */
	static Map<String, Object> readPermissions(User user, PermissionTargetResource target, boolean asAdmin)
			throws IllegalAccessException {
		String reason = findMissingTargetField(target);
		if (reason != null) {
			throw new IllegalArgumentException(reason);
		}
		boolean isProject = target.getKind() == PermissionTargetResource.Kind.PROJECT;
		boolean isTeam = isTeamKind(target);
		if (!isProject && !isTeam && target.getKind() != PermissionTargetResource.Kind.ENGINE) {
			throw new IllegalArgumentException("Only a project's, an engine's or a team's access can be read here");
		}

		List<Map<String, Object>> people = new ArrayList<>();
		for (Map<String, Object> row : readPeople(user, target, null, asAdmin)) {
			Map<String, Object> person = new LinkedHashMap<>();
			person.put("subject", personSubject(row));
			person.put("access", isTeam ? null : grantOf(row));
			people.add(person);
		}

		List<Map<String, Object>> groups = isTeam ? new ArrayList<>() : readGroups(user, target, asAdmin);
		List<Map<String, Object>> teams = new ArrayList<>();
		for (Map<String, Object> row : groups) {
			Map<String, Object> subject = new LinkedHashMap<>();
			subject.put("kind", PermissionSubject.Kind.TEAM.name());
			subject.put("id", row.get("ID"));
			subject.put("type", row.get("TYPE"));
			Map<String, Object> access = new LinkedHashMap<>();
			access.put("level", AccessPermissionEnum.getPermissionValueById(String.valueOf(row.get("PERMISSION"))));
			access.put("endDate", toIsoUtc(row.get("ENDDATE")));
			Map<String, Object> team = new LinkedHashMap<>();
			team.put("subject", subject);
			team.put("access", access);
			teams.add(team);
		}

		PermissionAllowedChanges allowed = readPermissionAllowedChanges(user, target, asAdmin);
		Map<String, Object> allowedChanges = new LinkedHashMap<>();
		allowedChanges.put("canChangePeople", allowed.canChangePeople());
		allowedChanges.put("canGiveAuthorAccess", allowed.isOwner());
		allowedChanges.put("canChangeTeams", allowed.canChangeTeams());
		allowedChanges.put("addsAreAdminOnly", allowed.addsAreAdminOnly());

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("people", people);
		result.put("teams", teams);
		result.put("allowedPermissionChanges", allowedChanges);
		return result;
	}

	/*
	 * Copying access
	 */

	/**
	 * @see SecurityAccessPermissionBatchUtils#getPermissionsToCopy(User,
	 *      PermissionTargetResource, PermissionTargetResource)
	 */
	static List<Map<String, Object>> readPermissionsToCopy(User user, PermissionTargetResource source,
			PermissionTargetResource target, boolean asAdmin) throws IllegalAccessException {
		String sourceReason = findMissingTargetField(source);
		String targetReason = findMissingTargetField(target);
		if (sourceReason != null || targetReason != null) {
			throw new IllegalArgumentException(sourceReason != null ? sourceReason : targetReason);
		}
		if (source.getKind() == PermissionTargetResource.Kind.INSIGHT
				|| target.getKind() == PermissionTargetResource.Kind.INSIGHT) {
			throw new IllegalArgumentException("Copying access to or from an insight is not supported yet");
		}
		// a team's members and managers can each be copied into either list of any
		// team, its own included, such as its managers into its members; managers
		// are never copied to or from a project or an engine
		boolean involvesManagers = source.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS
				|| target.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS;
		if (involvesManagers && !(isTeamKind(source) && isTeamKind(target))) {
			throw new IllegalArgumentException(
					"A team's managers can be copied only to or from a team's members or managers");
		}
		if (source.getKind() == target.getKind() && source.getId().equals(target.getId())) {
			throw new IllegalArgumentException("Pick a different " + nounFor(source) + " to copy from");
		}

		List<Map<String, Object>> sourceRows = readPeople(user, source, target, asAdmin);
		Map<String, Map<String, Object>> targetRows = new HashMap<>();
		for (Map<String, Object> row : readPeople(user, target, null, asAdmin)) {
			targetRows.put(personKey(target, row), row);
		}

		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map<String, Object> row : sourceRows) {
			Map<String, Object> inTarget = targetRows.get(personKey(target, row));
			Map<String, Object> copy = new LinkedHashMap<>();
			copy.put("subject", personSubject(row));
			copy.put("source", isTeamKind(source) ? null : grantOf(row));
			copy.put("target", inTarget == null || isTeamKind(target) ? null : grantOf(inTarget));
			copy.put("inTarget", inTarget != null);
			rows.add(copy);
		}
		return rows;
	}

	/**
	 * Reads the people of a project or engine, or a custom group's members or
	 * managers, in one shape: the keys {@code id}, {@code type}, {@code name},
	 * {@code email}, and for a project or engine {@code permission},
	 * {@code end_date} and the model usage limits.
	 *
	 * @param context for a group's members, the project or engine whose owners and
	 *                editors may read them, or null
	 * @param asAdmin whether in admin mode, which reads without checking who asks
	 */
	static List<Map<String, Object>> readPeople(User user, PermissionTargetResource from,
			PermissionTargetResource context, boolean asAdmin) throws IllegalAccessException {
		switch (from.getKind()) {
		case PROJECT:
			return asAdmin ? SecurityUserProjectUtils.getProjectUsers(from.getId(), null, null, -1, -1)
					: SecurityProjectUtils.getProjectUsers(user, from.getId(), null, null, -1, -1);
		case ENGINE:
			return asAdmin ? SecurityUserEngineUtils.getEngineUsers(from.getId(), null, null, -1, -1)
					: SecurityEngineUtils.getEngineUsers(user, from.getId(), null, null, -1, -1);
		case TEAM: {
			requireCanReadMembers(user, from.getId(), context, asAdmin);
			List<Map<String, Object>> people = new ArrayList<>();
			for (Map<String, Object> member : AdminSecurityGroupUtils.getUncheckedInstance()
					.getGroupMembers(from.getId(), null, -1, -1)) {
				Map<String, Object> person = new HashMap<>(member);
				person.put("id", member.get("userid"));
				people.add(person);
			}
			return people;
		}
		case TEAM_MANAGERS: {
			requireCanReadManagers(user, from.getId(), asAdmin);
			List<Map<String, Object>> people = new ArrayList<>();
			for (Map<String, Object> manager : SecurityGroupManagerUtils.queryGroupManagers(from.getId())) {
				Map<String, Object> person = new HashMap<>(manager);
				person.put("id", manager.get("userid"));
				people.add(person);
			}
			return people;
		}
		default:
			return new ArrayList<>();
		}
	}

	/**
	 * The groups that have access to a project or an engine: for anyone who can
	 * view it, or for anyone in admin mode
	 */
	static List<Map<String, Object>> readGroups(User user, PermissionTargetResource target, boolean asAdmin)
			throws IllegalAccessException {
		boolean isProject = target.getKind() == PermissionTargetResource.Kind.PROJECT;
		if (asAdmin) {
			return isProject ? SecurityGroupProjectUtils.queryGroupsWithAccessToProject(target.getId(), -1, -1)
					: SecurityGroupEngineUtils.queryGroupsWithAccessToEngine(target.getId(), -1, -1);
		}
		return isProject ? SecurityGroupProjectUtils.getGroupsWithAccessToProject(user, target.getId(), -1, -1)
				: SecurityGroupEngineUtils.getGroupsWithAccessToEngine(user, target.getId(), -1, -1);
	}

	/**
	 * Checks that the user may read a custom group's members: its managers, the
	 * owners of the project or engine being changed, its editors when the group can
	 * use it, or anyone in admin mode. Being an admin gives nothing outside admin
	 * mode. Who may read them is checked first, so someone who may not is told only
	 * that and cannot tell a missing team or a login provider group from a custom
	 * team.
	 *
	 * @throws IllegalArgumentException when the group is not a custom group, which
	 *                                  only those who may read it are told
	 * @throws IllegalAccessException   when the user may not read its members
	 */
	private static void requireCanReadMembers(User user, String groupId, PermissionTargetResource context,
			boolean asAdmin) throws IllegalAccessException {
		String projectId = context != null && context.getKind() == PermissionTargetResource.Kind.PROJECT
				? context.getId()
				: null;
		String engineId = context != null && context.getKind() == PermissionTargetResource.Kind.ENGINE ? context.getId()
				: null;
		boolean mayRead = asAdmin || SecurityGroupManagerUtils.userIsGroupManager(user, groupId)
				|| ((projectId != null || engineId != null)
						&& SecurityGroupManagerUtils.userCanViewGroupPeople(user, groupId, projectId, engineId));
		if (!mayRead) {
			throw new IllegalAccessException(
					"Only this team's managers, and the owners and editors of what it can use, can see its members");
		}
		if (!AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			throw new IllegalArgumentException(
					"Team " + groupId + " is not a custom team, so its members come from the login provider");
		}
	}

	/**
	 * Checks that the user may read a custom group's managers: its managers, or
	 * anyone in admin mode. Being an admin gives nothing outside admin mode. Who
	 * may read them is checked first, as for {@link #requireCanReadMembers}.
	 *
	 * @throws IllegalArgumentException when the group is not a custom group, which
	 *                                  only those who may read it are told
	 * @throws IllegalAccessException   when the user may not read its managers
	 */
	private static void requireCanReadManagers(User user, String groupId, boolean asAdmin)
			throws IllegalAccessException {
		if (!asAdmin && !SecurityGroupManagerUtils.userIsGroupManager(user, groupId)) {
			throw new IllegalAccessException("Only this team's managers can see who manages it");
		}
		if (!AdminSecurityGroupUtils.getUncheckedInstance().isCustomGroup(groupId)) {
			throw new IllegalArgumentException("Team " + groupId + " is not a custom team, so it has no managers");
		}
	}

	/**
	 * A project's and an engine's people are keyed by id alone, as their tables
	 * are; a group's members and managers by id and login type.
	 */
	private static String personKey(PermissionTargetResource target, Map<String, Object> person) {
		String id = String.valueOf(person.get("id"));
		return isTeamKind(target) ? id + ":" + person.get("type") : id;
	}

	private static Map<String, Object> personSubject(Map<String, Object> person) {
		Map<String, Object> subject = new LinkedHashMap<>();
		subject.put("kind", PermissionSubject.Kind.USER.name());
		subject.put("id", person.get("id"));
		subject.put("type", person.get("type"));
		subject.put("name", person.get("name"));
		subject.put("email", person.get("email"));
		return subject;
	}

	private static Map<String, Object> grantOf(Map<String, Object> person) {
		Map<String, Object> grant = new LinkedHashMap<>();
		grant.put("level", person.get("permission"));
		grant.put("endDate", toIsoUtc(person.get("end_date")));
		grant.put("usageRestriction", ValueUtils.trimToNull(person.get("usage_restriction")));
		grant.put("usageFrequency", ValueUtils.trimToNull(person.get("usage_frequency")));
		grant.put("maxTokens", person.get("max_tokens") instanceof Number number ? number.intValue() : null);
		grant.put("maxResponseTime",
				person.get("max_response_time") instanceof Number number ? number.doubleValue() : null);
		return grant;
	}

	/**
	 * End dates are stored in UTC; this writes one as an ISO date and time in UTC,
	 * which a batch can send back as it is.
	 *
	 * @return the date, such as {@code 2026-12-31T00:00Z}, or null
	 */
	static String toIsoUtc(Object value) {
		LocalDateTime local = null;
		if (value instanceof SemossDate date) {
			local = date.getLocalDateTime();
		} else if (value instanceof Timestamp timestamp) {
			local = timestamp.toLocalDateTime();
		} else if (value instanceof LocalDateTime dateTime) {
			local = dateTime;
		} else if (value instanceof java.util.Date date) {
			local = new Timestamp(date.getTime()).toLocalDateTime();
		} else if (value != null && !ValueUtils.isBlank(value.toString())) {
			try {
				local = LocalDateTime.parse(value.toString().trim().replace(' ', 'T'));
			} catch (DateTimeParseException e) {
				classLogger.warn("Could not read the end date {}", value);
			}
		}
		return local == null ? null : local.atOffset(ZoneOffset.UTC).toString();
	}

	/*
	 * Small helpers
	 */

	/**
	 * @return the id of an access level, or null for anything but {@code OWNER},
	 *         {@code EDIT} or {@code READ_ONLY}
	 */
	static Integer levelId(String level) {
		if (ValueUtils.isBlank(level)) {
			return null;
		}
		switch (level.trim().toUpperCase(Locale.ROOT)) {
		case "OWNER":
			return AccessPermissionEnum.OWNER.getId();
		case "EDIT":
			return AccessPermissionEnum.EDIT.getId();
		case "READ_ONLY":
			return AccessPermissionEnum.READ_ONLY.getId();
		default:
			return null;
		}
	}

	private static String targetKey(PermissionTargetResource target) {
		return target.getKind() + ":" + (target.getProjectId() == null ? "" : target.getProjectId() + ":")
				+ target.getId();
	}

	private static String subjectKey(PermissionTargetResource target, PermissionChangeValidation validation) {
		PermissionSubject subject = validation.getChange().getSubject();
		if (subject.getKind() == PermissionSubject.Kind.TEAM) {
			return "TEAM:" + validation.getSubjectId() + ":" + validation.getSubjectType();
		}
		// a project's, engine's or insight's people are keyed by id alone
		return isTeamKind(target) ? "USER:" + validation.getSubjectId() + ":" + validation.getSubjectType()
				: "USER:" + validation.getSubjectId();
	}

	/**
	 * @return whether the target is a custom group's members or managers, whose
	 *         people have a login type and no access level
	 */
	private static boolean isTeamKind(PermissionTargetResource target) {
		return target.getKind() == PermissionTargetResource.Kind.TEAM
				|| target.getKind() == PermissionTargetResource.Kind.TEAM_MANAGERS;
	}

	private static String nounFor(PermissionTargetResource target) {
		if (target == null || target.getKind() == null) {
			return "resource";
		}
		return isTeamKind(target) ? "team" : target.getKind().name().toLowerCase(Locale.ROOT);
	}
}
