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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionProblem;
import prerna.auth.access.PermissionTargetResource;

/**
 * The validation of one permission batch: a {@link PermissionChangeValidation}
 * for each of its changes, and a reason when the whole batch is blocked, such
 * as a target that does not exist or a user with no permission on it. Saving a
 * batch works from this, not from the {@link PermissionBatch} as sent. Only
 * {@link PermissionBatchUtils} makes them, in {@code validateBatch}.
 */
final class PermissionBatchValidation {

	private final PermissionTargetResource target;
	private final List<PermissionChangeValidation> changes = new ArrayList<>();
	/** Why the whole batch cannot be made, or null */
	private String reason;
	/** The type of problem, when the whole batch cannot be made */
	private PermissionProblem.ErrorCode errorCode;

	/**
	 * Starts the validation of a batch, with a {@link PermissionChangeValidation}
	 * for each of its changes, in order.
	 *
	 * @param batch the batch as sent
	 */
	PermissionBatchValidation(PermissionBatch batch) {
		this.target = batch.getTarget();
		List<PermissionChange> batchChanges = batch.getChanges();
		for (int i = 0; i < batchChanges.size(); i++) {
			changes.add(new PermissionChangeValidation(i, batchChanges.get(i)));
		}
	}

	/**
	 * Notes why the whole batch cannot be made, so none of it is saved.
	 *
	 * @param errorCode the type of problem
	 * @param reason    the message to show
	 */
	void block(PermissionProblem.ErrorCode errorCode, String reason) {
		this.errorCode = errorCode;
		this.reason = reason;
	}

	/**
	 * Whether a reason has been given to block the whole batch, so none of it is
	 * saved.
	 *
	 * @return whether the whole batch cannot be made
	 */
	boolean isBlocked() {
		return reason != null;
	}

	PermissionTargetResource getTarget() {
		return target;
	}

	/**
	 * The validations of the batch's changes, which reading and validating fill in.
	 *
	 * @return the validation of each change, in the batch's order
	 */
	List<PermissionChangeValidation> getChanges() {
		return changes;
	}

	/**
	 * Lists a {@link PermissionProblem} for the whole batch, when it is blocked,
	 * and one for each blocked change, with its index and subject id.
	 *
	 * @return every problem found: the whole batch's first, then each change's
	 */
	List<PermissionProblem> problems() {
		List<PermissionProblem> problems = new ArrayList<>();
		if (reason != null) {
			problems.add(new PermissionProblem(errorCode, reason, target, null));
		}
		for (PermissionChangeValidation change : changes) {
			if (change.isBlocked()) {
				problems.add(new PermissionProblem(change.getErrorCode(), change.getReason(), target, change.getIndex(),
						change.getSubjectId()));
			}
		}
		return problems;
	}

	/**
	 * Builds the result {@code validateBatches} returns for the batch. When the
	 * whole batch is blocked, every change is not allowed and gets the batch's
	 * reason.
	 *
	 * @return the batch's target and, for each change, its index, whether it is
	 *         allowed, and why not, as Bulk Access Management's preview returns
	 *         them
	 */
	Map<String, Object> toResult() {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("target", target);
		if (reason != null) {
			result.put("reason", reason);
		}
		List<Map<String, Object>> results = new ArrayList<>();
		for (PermissionChangeValidation change : changes) {
			Map<String, Object> changeResult = new LinkedHashMap<>();
			changeResult.put("index", change.getIndex());
			changeResult.put("allowed", reason == null && !change.isBlocked());
			changeResult.put("reason", reason != null ? reason : change.getReason());
			results.add(changeResult);
		}
		result.put("changes", results);
		return result;
	}
}
