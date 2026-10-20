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

import org.javatuples.Pair;

import prerna.auth.access.PermissionTargetResource;

/**
 * How permission batches are checked and saved. Bulk Access Management uses
 * {@link #bulkAccessManagement(boolean)}. The single and bulk permission
 * methods, such as {@code SecurityProjectUtils.addProjectUser}, start from
 * {@link #userMode()} or {@link #adminMode()} and turn on what keeps their
 * behavior, such as {@link #allowingSelfRemoval()}.
 *
 * <p>
 * The options only describe a save; {@link PermissionBatchUtils} reads them
 * while it checks and saves the batches. They are not part of the batches sent
 * as JSON. They are package-private, so only this package can turn on admin
 * mode, and only for callers that hold the admin utilities.
 * </p>
 */
final class PermissionBatchOptions {

	/**
	 * Whether in admin mode, where every change the shape and the current access
	 * allow is allowed
	 */
	private boolean adminMode;
	/** Whether the caller records one bulk audit summary after the shared save. */
	private boolean summaryAudit;
	/** Whether people given access to a project or engine are notified */
	private boolean notifyAdds = true;
	/** Whether people whose access to a project or engine changes are notified */
	private boolean notifyChanges = true;
	/**
	 * Whether a level is read as
	 * {@link prerna.auth.AccessPermissionEnum#getIdByPermission} reads it, so
	 * anything but a level's name is Read-Only
	 */
	private boolean lenientLevels;
	/**
	 * Whether a change on an engine whose grant does not give its usage limit keeps
	 * the limit of the person's or the team's grant rather than clearing it
	 */
	private boolean keepModelLimits;
	/**
	 * Whether people may take away their own access to a project or engine,
	 * whatever their level
	 */
	private boolean allowSelfRemoval;
	/**
	 * Whether taking someone off a project also takes away their permissions on its
	 * insights
	 */
	private boolean removeInsightPermissions;
	/** Whether a team given access must exist */
	private boolean checkTeamExists = true;
	/** Whether, in admin mode, a project, engine or insight must exist */
	private boolean checkTargetExists = true;
	/**
	 * Whether the admin-only add settings refuse an add here; the endpoints of the
	 * single and bulk permission methods check them before calling them
	 */
	private boolean enforceAdminOnlyAdds = true;
	/**
	 * Whether a team's type is kept as given, rather than as the stored provider
	 * label
	 */
	private boolean keepTeamType;
	/** Whether giving a team the level it already has is refused */
	private boolean rejectSameLevel;
	/**
	 * Who the rows say gave the access, when it is not the user: an id and a login
	 * type
	 */
	private Pair<String, String> grantor;
	/** The target whose access requests the batch approves, or null */
	private PermissionTargetResource approvedTarget;
	/**
	 * The access requests the batch approves, marked in the same transaction, or
	 * null
	 */
	private List<? extends Map<String, ?>> approvedRequests;

	private PermissionBatchOptions() {

	}

	/**
	 * Makes the options Bulk Access Management checks and saves its batches with.
	 *
	 * @param adminMode whether in admin mode
	 * @return Bulk Access Management's options: every add and change notifies, only
	 *         a level's name is a level, and in admin mode the target must exist
	 */
	static PermissionBatchOptions bulkAccessManagement(boolean adminMode) {
		PermissionBatchOptions options = new PermissionBatchOptions();
		options.adminMode = adminMode;
		return options;
	}

	/**
	 * Makes the options the single and bulk permission methods start from when they
	 * act with the user's own permissions.
	 *
	 * @return the starting options of the single and bulk permission methods, with
	 *         the user's own permissions: no notifications, levels read as those
	 *         methods read them, the admin-only add settings left to their
	 *         endpoints, and no check that a target exists
	 */
	static PermissionBatchOptions userMode() {
		return permissionMethod(false);
	}

	/**
	 * Makes the starting options of the admin utilities' single and bulk permission
	 * methods, which save in admin mode.
	 *
	 * @return {@link #userMode()} in admin mode, for the methods of
	 *         {@code SecurityAdminUtils} and {@code AdminSecurityGroupUtils}
	 */
	static PermissionBatchOptions adminMode() {
		return permissionMethod(true);
	}

	private static PermissionBatchOptions permissionMethod(boolean adminMode) {
		PermissionBatchOptions options = bulkAccessManagement(adminMode);
		options.notifyAdds = false;
		options.notifyChanges = false;
		options.lenientLevels = true;
		options.checkTargetExists = false;
		options.enforceAdminOnlyAdds = false;
		return options;
	}

	/**
	 * Sets whether the save notifies the people it gives access to a project or an
	 * engine, and the people whose access to one it changes.
	 *
	 * @param adds    whether people given access are notified
	 * @param changes whether people whose access changes are notified
	 * @return these options
	 */
	PermissionBatchOptions notifying(boolean adds, boolean changes) {
		this.notifyAdds = adds;
		this.notifyChanges = changes;
		return this;
	}

	/**
	 * A change on an engine whose grant does not give its usage limit
	 * ({@link prerna.auth.access.PermissionGrant#isUsageLimitGiven()} is false and
	 * the limit is null), such as a change of level only or a bulk row without a
	 * {@code usageLimit} key, keeps the usage limit of the person's or the team's
	 * grant as it is. A grant that gives its limit, null included, sets it.
	 *
	 * <p>
	 * The engine methods that edit a person's or a team's level and end date,
	 * single and bulk, turn it on, so an edit that does not mention the limit keeps
	 * it. That also lets an editor change a level there, since only owners may
	 * change a limit. Only a {@code CHANGE} keeps one; an add gives the limit its
	 * grant gives.
	 * </p>
	 *
	 * @return these options
	 */
	PermissionBatchOptions keepingModelLimits() {
		this.keepModelLimits = true;
		return this;
	}

	/**
	 * Lets people take away their own access to a project or an engine with no
	 * other permission there, whatever their level.
	 *
	 * @return these options, where anyone can take away their own access to a
	 *         project or an engine
	 */
	PermissionBatchOptions allowingSelfRemoval() {
		this.allowSelfRemoval = true;
		return this;
	}

	/**
	 * Makes taking someone off a project also delete their permissions on the
	 * project's insights, in the same transaction.
	 *
	 * @return these options, where taking someone off a project also takes away
	 *         their permissions on its insights
	 */
	PermissionBatchOptions removingInsightPermissions() {
		this.removeInsightPermissions = true;
		return this;
	}

	/**
	 * Skips the check that a team given access to a project, an engine or an
	 * insight is registered.
	 *
	 * @return these options, where a team need not be registered to be given access
	 */
	PermissionBatchOptions withoutTeamExistsCheck() {
		this.checkTeamExists = false;
		return this;
	}

	/**
	 * Stores a team's type exactly as the change gives it, rather than as the label
	 * of the login provider the type names.
	 *
	 * @return these options, where a team's type is stored as given
	 */
	PermissionBatchOptions keepingTeamType() {
		this.keepTeamType = true;
		return this;
	}

	/**
	 * Refuses, as {@code SAME_LEVEL}, a change that gives a team the level it
	 * already has.
	 *
	 * @return these options, where giving a team the level it has is refused
	 */
	PermissionBatchOptions rejectingSameLevel() {
		this.rejectSameLevel = true;
		return this;
	}

	/**
	 * Names who the saved rows say gave the access, or denied the access requests,
	 * when it is not the user.
	 *
	 * @param grantorId   who the rows say gave the access
	 * @param grantorType their login type
	 * @return these options
	 */
	PermissionBatchOptions grantedBy(String grantorId, String grantorType) {
		this.grantor = new Pair<>(grantorId, grantorType);
		return this;
	}

	/**
	 * Marks access requests approved in the same transaction as the changes, by
	 * whoever the rows say gave the access, each with the level it asked for. A
	 * request asks only for a level, so a person who has access already keeps their
	 * usage limit on a model ({@link #keepingModelLimits()}).
	 *
	 * @param target   the project, engine or insight
	 * @param requests the requests, each with {@code requestid} and
	 *                 {@code permission}
	 * @return these options
	 */
	PermissionBatchOptions approving(PermissionTargetResource target, List<? extends Map<String, ?>> requests) {
		this.approvedTarget = target;
		this.approvedRequests = requests;
		this.keepModelLimits = true;
		return this;
	}

	boolean isAdminMode() {
		return adminMode;
	}

	/** Keep the existing bulk event instead of also recording each permission row. */
	PermissionBatchOptions withSummaryAudit() {
		this.summaryAudit = true;
		return this;
	}

	boolean hasSummaryAudit() {
		return summaryAudit;
	}

	boolean notifiesAdds() {
		return notifyAdds;
	}

	boolean notifiesChanges() {
		return notifyChanges;
	}

	boolean readsLevelsLeniently() {
		return lenientLevels;
	}

	boolean keepsModelLimits() {
		return keepModelLimits;
	}

	boolean allowsSelfRemoval() {
		return allowSelfRemoval;
	}

	boolean removesInsightPermissions() {
		return removeInsightPermissions;
	}

	boolean checksTeamExists() {
		return checkTeamExists;
	}

	boolean checksTargetExists() {
		return checkTargetExists;
	}

	boolean enforcesAdminOnlyAdds() {
		return enforceAdminOnlyAdds;
	}

	boolean keepsTeamType() {
		return keepTeamType;
	}

	boolean rejectsSameLevel() {
		return rejectSameLevel;
	}

	/**
	 * The id and login type the saved rows name as giving the access, set by
	 * {@link #grantedBy}.
	 *
	 * @return who the rows say gave the access, or null for the user
	 */
	Pair<String, String> getGrantor() {
		return grantor;
	}

	/**
	 * The project, engine or insight whose access requests the save marks approved,
	 * set by {@link #approving}.
	 *
	 * @return the target whose access requests the batch approves, or null
	 */
	PermissionTargetResource getApprovedTarget() {
		return approvedTarget;
	}

	/**
	 * The access requests the save marks approved in its own transaction, set by
	 * {@link #approving}.
	 *
	 * @return the access requests the batch approves, or null
	 */
	List<? extends Map<String, ?>> getApprovedRequests() {
		return approvedRequests;
	}
}
