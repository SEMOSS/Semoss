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
package prerna.auth.access;

/**
 * Why a change in a permission batch cannot be made: a code to act on, and the
 * message to show.
 */
public class PermissionProblem {

	/** The kinds of problem */
	public enum Code {
		/** The batch or the change does not say what it needs to */
		NOT_WELL_FORMED,
		/** An end date is not a valid date */
		INVALID_DATE,
		/** The same subject is in the batch more than once */
		DUPLICATE,
		/** The target does not exist, or cannot be changed this way */
		INVALID_TARGET,
		/** The user may not make the change */
		NO_PERMISSION,
		/** Only admins can give people access to the target */
		ADMIN_ONLY,
		/** Only owners can give Author access */
		GIVES_AUTHOR,
		/** Only owners can change or take away an Author's access */
		CHANGES_AUTHOR,
		/** Only the target's owners can do this, such as deny access requests */
		OWNERS_ONLY,
		/** The subject already has the access, or is already in the team */
		ALREADY_HAS_ACCESS,
		/** The subject no longer has the access to change or take away */
		NO_ACCESS,
		/** The change gives the level the subject already has */
		SAME_LEVEL,
		/** The person or team does not exist */
		SUBJECT_MISSING,
		/** The changes could not be saved */
		WRITE_FAILED
	}

	private final Code code;
	private final String message;
	private final PermissionTargetResource target;
	private final Integer index;
	private final String subjectId;

	/**
	 * @param code    the kind of problem
	 * @param message what to show
	 * @param target  the target of the batch it is in, or null
	 * @param index   the change's index in its batch, or null for the whole batch
	 */
	public PermissionProblem(Code code, String message, PermissionTargetResource target, Integer index) {
		this(code, message, target, index, null);
	}

	/**
	 * @param code      the kind of problem
	 * @param message   what to show
	 * @param target    the target of the batch it is in, or null
	 * @param index     the change's index in its batch, or null for the whole batch
	 * @param subjectId the id of the person or team the change is for, or null
	 */
	public PermissionProblem(Code code, String message, PermissionTargetResource target, Integer index,
			String subjectId) {
		this.code = code;
		this.message = message;
		this.target = target;
		this.index = index;
		this.subjectId = subjectId;
	}

	public Code getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}

	public PermissionTargetResource getTarget() {
		return target;
	}

	public Integer getIndex() {
		return index;
	}

	public String getSubjectId() {
		return subjectId;
	}
}
