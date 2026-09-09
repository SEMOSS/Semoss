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
package prerna.util.git;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Template for the git working-tree reactors exposed for both engines and
 * projects. Owns the whole shell of a request: key organization, the anonymous
 * user guard, target id validation, the permission check, version folder
 * resolution, opening the repository, and error translation. Subclasses supply
 * only the operation itself through
 * {@link #runGitOperation(Git, GitTargetHandle)}, and the engine/project
 * difference arrives as a {@link GitReactorTarget}.
 */
public abstract class AbstractGitWorktreeReactor extends AbstractReactor {

	protected final Logger classLogger = LogManager.getLogger(getClass());

	protected final GitReactorTarget target;

	/**
	 * @param target                the engine or project flavor this reactor runs
	 *                              against
	 * @param operationKeys         the keys this operation accepts on top of the
	 *                              target id key, which is always first
	 * @param operationKeysRequired the required flags aligned to
	 *                              {@code operationKeys}
	 */
	protected AbstractGitWorktreeReactor(GitReactorTarget target, String[] operationKeys, int[] operationKeysRequired) {
		this.target = target;
		this.keysToGet = new String[operationKeys.length + 1];
		this.keyRequired = new int[operationKeysRequired.length + 1];
		this.keysToGet[0] = target.getIdKey();
		this.keyRequired[0] = 1;
		System.arraycopy(operationKeys, 0, this.keysToGet, 1, operationKeys.length);
		System.arraycopy(operationKeysRequired, 0, this.keyRequired, 1, operationKeysRequired.length);
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (requiresEditPermission() && AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String targetId = this.keyValue.get(this.target.getIdKey());
		if (targetId == null || (targetId = targetId.trim()).isEmpty()) {
			throw new SemossPixelException("Must pass in the " + this.target.getLabel() + " id");
		}
		validateOperationInput();

		boolean allowed = requiresEditPermission() ? this.target.userCanEdit(user, targetId)
				: this.target.userCanView(user, targetId);
		if (!allowed) {
			throw new SemossPixelException(this.target.getCapitalizedLabel()
					+ " does not exist or user does not have access to the " + this.target.getLabel());
		}

		GitTargetHandle handle = this.target.resolve(targetId);
		Map<String, Object> resultMap;
		File gitDir = new File(handle.getVersionFolder(), ".git");
		if (!gitDir.exists()) {
			resultMap = getResultWithoutRepo(handle);
		} else {
			try (Git thisGit = Git.open(new File(handle.getVersionFolder()))) {
				resultMap = runGitOperation(thisGit, handle);
			} catch (Exception e) {
				if (e instanceof SemossPixelException) {
					throw (SemossPixelException) e;
				}
				if (e instanceof IllegalArgumentException && surfacesIllegalArgumentAsUserError()) {
					throw new SemossPixelException(e.getMessage());
				}
				classLogger.error("Error occurred {} for {} {}", getOperationLogPhrase(), this.target.getLabel(),
						targetId, e);
				throw new SemossPixelException(getOperationErrorMessage() + " Detailed error = " + e.getMessage(), e);
			}
		}

		return new NounMetadata(resultMap, PixelDataType.MAP, this.target.getOpType());
	}

	/**
	 * Runs the operation against an open repository. Throwing a
	 * {@link SemossPixelException} surfaces the message as-is; anything else is
	 * logged and wrapped in {@link #getOperationErrorMessage()}.
	 */
	protected abstract Map<String, Object> runGitOperation(Git thisGit, GitTargetHandle handle) throws Exception;

	/**
	 * Whether the caller needs edit rather than view access. Operations that
	 * require edit access also reject anonymous users.
	 */
	protected abstract boolean requiresEditPermission();

	/**
	 * Present participle naming the operation in logs, e.g. "checking out branch".
	 */
	protected abstract String getOperationLogPhrase();

	/**
	 * Sentence prefixed to the detailed error when the operation fails
	 * unexpectedly, e.g. "Error occurred checking out the branch."
	 */
	protected abstract String getOperationErrorMessage();

	/**
	 * Validates and captures the operation specific keys. Runs after the target id
	 * is validated and before the permission check, so bad input is rejected
	 * without a security lookup.
	 */
	protected void validateOperationInput() {
		// nothing beyond the target id by default
	}

	/**
	 * Builds the response when the target has no git repository yet. Refuses by
	 * default; read-only operations that have a meaningful empty answer override
	 * this.
	 */
	protected Map<String, Object> getResultWithoutRepo(GitTargetHandle handle) {
		throw new SemossPixelException(this.target.getCapitalizedLabel() + " does not have a git repository yet");
	}

	/**
	 * Whether an {@link IllegalArgumentException} out of the operation body is a
	 * caller input error whose message should be surfaced directly, as it is for
	 * repo-relative path validation. Otherwise it is logged and wrapped like any
	 * other unexpected failure.
	 */
	protected boolean surfacesIllegalArgumentAsUserError() {
		return false;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(this.target.getIdKey())) {
			return "The " + this.target.getLabel() + " id";
		}
		return super.getDescriptionForKey(key);
	}
}
