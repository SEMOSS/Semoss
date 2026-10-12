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
 * fills in the subject's level in those rows.</li>
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
	 * Whether the person has a favorite or visibility row with no level, which an
	 * add fills in
	 */
	private boolean hasPreferenceRow;
	/** Why the change cannot be made, or null while it can */
	private String reason;
	/** The kind of problem, when the change cannot be made */
	private PermissionProblem.Code code;

	/**
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
	 * @param code   the kind of problem
	 * @param reason the message to show
	 */
	void block(PermissionProblem.Code code, String reason) {
		this.code = code;
		this.reason = reason;
	}

	/**
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
	 * @return why the change cannot be made, or null while it can
	 */
	String getReason() {
		return reason;
	}

	/**
	 * @return the kind of problem, or null while the change can be made
	 */
	PermissionProblem.Code getCode() {
		return code;
	}
}
