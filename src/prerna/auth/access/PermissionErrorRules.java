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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The exception and message one of the single or bulk permission methods throws
 * for each type of {@link PermissionProblem}, so its callers keep the exception
 * classes and messages they handle. For example,
 * {@code SecurityProjectUtils.removeProjectUsers} throws "Attempting to modify
 * user permission for the following users who do not currently have access to
 * the project: bob" for a {@link PermissionProblem.ErrorCode#NO_ACCESS}
 * problem.
 *
 * <p>
 * Each rule gives the exception for one type of problem. When a save fails,
 * {@link #toException(List)} uses the first rule, in the order they were added,
 * that any of the problems matches. Problems no rule matches get the messages
 * every method shares: only admins can give access, someone does not exist,
 * someone is listed twice, or an end date cannot be read. Anything else gets
 * the batch's own {@link PermissionBatchException}.
 * </p>
 */
public final class PermissionErrorRules {

	/** The end date the method was given, named when it cannot be read */
	private final String endDate;
	private final Map<PermissionProblem.ErrorCode, Function<List<PermissionProblem>, Exception>> rules = new LinkedHashMap<>();

	/**
	 * No rules of the method's own yet, for a method with no end date.
	 */
	public PermissionErrorRules() {
		this(null);
	}

	/**
	 * No rules of the method's own yet.
	 *
	 * @param endDate the end date the method was given, which the message for one
	 *                that cannot be read names; or null to use the batch's message
	 */
	public PermissionErrorRules(String endDate) {
		this.endDate = endDate;
	}

	/**
	 * Builds the rules for approving access requests: the messages for giving or
	 * changing Author access as a non-owner, and for a failed write.
	 *
	 * @param endDate the end date the approval was given, or null
	 * @return the rules every access request approval method shares; the user-mode
	 *         methods add their own for a user with no permission
	 */
	public static PermissionErrorRules forApprovals(String endDate) {
		return new PermissionErrorRules(endDate)
				.illegalArgument(PermissionProblem.ErrorCode.GIVES_AUTHOR,
						"As a non-owner, you cannot grant owner access.")
				.illegalArgument(PermissionProblem.ErrorCode.CHANGES_AUTHOR,
						"As a non-owner, you cannot edit access of an owner.")
				.illegalArgument(PermissionProblem.ErrorCode.WRITE_FAILED,
						"An error occurred while approving the access requests");
	}

	/**
	 * Adds a rule that throws an {@link IllegalAccessException} with the given
	 * message when any problem has the code.
	 *
	 * @return these rules, giving an {@link IllegalAccessException} with the
	 *         message for the type of problem
	 */
	public PermissionErrorRules illegalAccess(PermissionProblem.ErrorCode errorCode, String message) {
		rules.put(errorCode, problems -> new IllegalAccessException(message));
		return this;
	}

	/**
	 * Adds a rule that throws an {@link IllegalArgumentException} with the given
	 * message when any problem has the code.
	 *
	 * @return these rules, giving an {@link IllegalArgumentException} with the
	 *         message for the type of problem
	 */
	public PermissionErrorRules illegalArgument(PermissionProblem.ErrorCode errorCode, String message) {
		rules.put(errorCode, problems -> new IllegalArgumentException(message));
		return this;
	}

	/**
	 * {@link #illegalArgument(PermissionProblem.ErrorCode, String)} for a message
	 * that reads the current rows, which is built only when the problem is found.
	 */
	public PermissionErrorRules illegalArgument(PermissionProblem.ErrorCode errorCode, Supplier<String> message) {
		rules.put(errorCode, problems -> new IllegalArgumentException(message.get()));
		return this;
	}

	/**
	 * {@link #illegalArgument(PermissionProblem.ErrorCode, String)} for a bulk
	 * method: the message is followed by the ids of everyone with the problem,
	 * joined with commas.
	 */
	public PermissionErrorRules illegalArgumentWithIds(PermissionProblem.ErrorCode errorCode, String message) {
		rules.put(errorCode, problems -> new IllegalArgumentException(message + idsWith(problems, errorCode)));
		return this;
	}

	/**
	 * Picks the exception for the problems: the first rule, in the order added,
	 * that any problem matches, or else the shared messages for admin-only adds,
	 * people listed twice, people who do not exist, and end dates that cannot be
	 * read.
	 *
	 * @param problems every problem found, the first one first
	 * @return the exception the method throws for them, or null for the batch's own
	 */
	public Exception toException(List<PermissionProblem> problems) {
		for (Map.Entry<PermissionProblem.ErrorCode, Function<List<PermissionProblem>, Exception>> rule : rules
				.entrySet()) {
			if (has(problems, rule.getKey())) {
				return rule.getValue().apply(problems);
			}
		}
		if (has(problems, PermissionProblem.ErrorCode.ADMIN_ONLY)) {
			return new IllegalAccessException("This functionality is limited to only admins");
		}
		if (has(problems, PermissionProblem.ErrorCode.DUPLICATE)) {
			return new IllegalArgumentException("The following users are listed more than once: "
					+ idsWith(problems, PermissionProblem.ErrorCode.DUPLICATE));
		}
		if (has(problems, PermissionProblem.ErrorCode.SUBJECT_MISSING)) {
			String ids = idsWith(problems, PermissionProblem.ErrorCode.SUBJECT_MISSING);
			return new IllegalArgumentException(
					ids.contains(",") ? "The following users do not exist: " + ids : "User " + ids + " does not exist");
		}
		for (PermissionProblem problem : problems) {
			if (problem.getErrorCode() == PermissionProblem.ErrorCode.INVALID_DATE) {
				return new IllegalArgumentException(
						endDate == null ? problem.getMessage() : "The end date " + endDate + " is not a valid date");
			}
		}
		return null;
	}

	/**
	 * Lists the subject ids of the problems with the code, skipping problems with
	 * no subject id.
	 *
	 * @return the ids of the people or teams with the type of problem, joined with
	 *         commas, as the bulk methods list them
	 */
	private static String idsWith(List<PermissionProblem> problems, PermissionProblem.ErrorCode errorCode) {
		List<String> ids = new ArrayList<>();
		for (PermissionProblem problem : problems) {
			if (problem.getErrorCode() == errorCode && problem.getSubjectId() != null) {
				ids.add(problem.getSubjectId());
			}
		}
		return String.join(",", ids);
	}

	private static boolean has(List<PermissionProblem> problems, PermissionProblem.ErrorCode errorCode) {
		for (PermissionProblem problem : problems) {
			if (problem.getErrorCode() == errorCode) {
				return true;
			}
		}
		return false;
	}
}
