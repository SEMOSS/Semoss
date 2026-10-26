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

import java.sql.Timestamp;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionProblem;

/**
 * The validation of one change of a permission batch. Saving a batch works from
 * these, not from the {@link PermissionChange} as sent:
 * <ol>
 * <li>It starts with only the change as sent.</li>
 * <li>Reading the change fills in the subject's id and type, the level and the
 * end date.</li>
 * <li>Validating it against the user's permissions and the rows that exist
 * fills in the subject's level in those rows and, on a model, the usage limit
 * the change leaves.</li>
 * <li>Either step can block it, with a code and a reason. A change that is not
 * blocked is written from the values filled in.</li>
 * </ol>
 * Only {@link PermissionBatchUtils} makes and fills them in.
 */
final class PermissionChangeValidation {

	/** The change's index in its batch */
	private final int index;
	/** The change as sent */
	private final PermissionChange change;
	/** The subject's id, trimmed */
	private String subjectId;
	/** The person's login type, or the team's type, as stored */
	private String subjectType;
	/** The level the change gives, as an id */
	private Integer levelId;
	/**
	 * The highest level in the subject's rows now, ended or not, as an id, or null
	 * for none
	 */
	private Integer currentLevelId;
	/** When the access ends, or null for never */
	private Timestamp endDate;
	/**
	 * Whether the person is picked from the Microsoft directory and needs an
	 * account first
	 */
	private boolean needsDirectoryAccount;
	/**
	 * Whether the person has a permission row with no level, which counts as no
	 * access. An add fills that row in rather than adding a second one.
	 */
	private boolean hasPreferenceRow;
	/**
	 * Where the change writes a usage limit: a person's or team's grant on a model,
	 * its public use or its total; null when it writes none, such as on a project,
	 * an engine that is not a model, or a change that keeps the limit. On a removal
	 * from a model it is the grant whose limit is deleted with it.
	 */
	private ModelUsageLimitKey limitKey;
	/**
	 * The usage limit after the change, normalized, or null for no limit defined
	 */
	private ModelUsageLimit usageLimit;
	/**
	 * The usage limit before the change, on a model's grant, public use or total,
	 * or null for no limit defined; the audit trail records it beside the new one
	 */
	private ModelUsageLimit currentUsageLimit;
	/**
	 * Whether the change leaves the current usage limit as it is, so it writes no
	 * limit and needs no permission to set one. This is how an editor's change of
	 * level passes, since only owners set usage limits.
	 */
	private boolean keepsUsageLimit;
	/** Why the change cannot be made, or null while it can */
	private String reason;
	/** The type of problem, when the change cannot be made */
	private PermissionProblem.ErrorCode errorCode;

	/**
	 * Starts the validation of a change with only the change as sent, before it is
	 * read.
	 *
	 * @param index  the change's index in its batch
	 * @param change the change as sent
	 */
	PermissionChangeValidation(int index, PermissionChange change) {
		this.index = index;
		this.change = change;
	}

	/**
	 * Notes why the change cannot be made, so it is not saved.
	 *
	 * @param code   the type of problem
	 * @param reason the message to show
	 */
	void block(PermissionProblem.ErrorCode errorCode, String reason) {
		this.errorCode = errorCode;
		this.reason = reason;
	}

	/**
	 * Whether a reason has been given to block the change, so it is not saved.
	 *
	 * @return whether the change cannot be made
	 */
	boolean isBlocked() {
		return reason != null;
	}

	int getIndex() {
		return index;
	}

	PermissionChange getChange() {
		return change;
	}

	String getSubjectId() {
		return subjectId;
	}

	void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}

	String getSubjectType() {
		return subjectType;
	}

	void setSubjectType(String subjectType) {
		this.subjectType = subjectType;
	}

	Integer getLevelId() {
		return levelId;
	}

	void setLevelId(Integer levelId) {
		this.levelId = levelId;
	}

	Integer getCurrentLevelId() {
		return currentLevelId;
	}

	void setCurrentLevelId(Integer currentLevelId) {
		this.currentLevelId = currentLevelId;
	}

	Timestamp getEndDate() {
		return endDate;
	}

	void setEndDate(Timestamp endDate) {
		this.endDate = endDate;
	}

	boolean needsDirectoryAccount() {
		return needsDirectoryAccount;
	}

	void setNeedsDirectoryAccount(boolean needsDirectoryAccount) {
		this.needsDirectoryAccount = needsDirectoryAccount;
	}

	boolean hasPreferenceRow() {
		return hasPreferenceRow;
	}

	void setHasPreferenceRow(boolean hasPreferenceRow) {
		this.hasPreferenceRow = hasPreferenceRow;
	}

	/**
	 * The key of the usage limit the change writes, or, on a removal from a model,
	 * of the grant whose limit is deleted with it.
	 *
	 * @return where the change writes a usage limit, or null when it writes none
	 */
	ModelUsageLimitKey getLimitKey() {
		return limitKey;
	}

	void setLimitKey(ModelUsageLimitKey limitKey) {
		this.limitKey = limitKey;
	}

	/**
	 * The usage limit the subject has after the change, either the one it sets or
	 * the current one it keeps.
	 *
	 * @return the usage limit after the change, or null for no limit defined
	 */
	ModelUsageLimit getUsageLimit() {
		return usageLimit;
	}

	/**
	 * Sets the usage limit the change writes, so it does not keep the current one.
	 *
	 * @param usageLimit the usage limit after the change, normalized, or null for
	 *                   no limit defined
	 */
	void setUsageLimit(ModelUsageLimit usageLimit) {
		this.usageLimit = usageLimit;
		this.keepsUsageLimit = false;
	}

	/**
	 * The usage limit the grant, public use or total has before the change.
	 *
	 * @return the usage limit before the change, or null for no limit defined
	 */
	ModelUsageLimit getCurrentUsageLimit() {
		return currentUsageLimit;
	}

	/**
	 * Notes the usage limit before the change, as read for the change.
	 *
	 * @param currentUsageLimit the usage limit now, or null for no limit defined
	 */
	void setCurrentUsageLimit(ModelUsageLimit currentUsageLimit) {
		this.currentUsageLimit = currentUsageLimit;
	}

	/**
	 * Whether the change leaves a usage limit different from the one before it,
	 * which the audit trail records: a limit set, changed or removed, a grant's
	 * removal with it included.
	 *
	 * @return whether the change writes a limit and the limit before or after it is
	 *         defined
	 */
	boolean changesUsageLimit() {
		return limitKey != null && !keepsUsageLimit && (currentUsageLimit != null || usageLimit != null);
	}

	/**
	 * Leaves the current usage limit as it is, so the change writes none.
	 *
	 * @param current the current limit, or null
	 */
	void keepUsageLimit(ModelUsageLimit current) {
		this.usageLimit = current;
		this.keepsUsageLimit = true;
	}

	/**
	 * Whether the change keeps the current usage limit, so it writes no limit and
	 * needs no permission to set one.
	 *
	 * @return whether the change leaves the current usage limit as it is
	 */
	boolean keepsUsageLimit() {
		return keepsUsageLimit;
	}

	/**
	 * The message to show for a blocked change.
	 *
	 * @return why the change cannot be made, or null while it can
	 */
	String getReason() {
		return reason;
	}

	/**
	 * The problem code for a blocked change.
	 *
	 * @return the type of problem, or null while the change can be made
	 */
	PermissionProblem.ErrorCode getErrorCode() {
		return errorCode;
	}
}
