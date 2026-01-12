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
package prerna.util.git.reactors;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.RefAlreadyExistsException;
import org.eclipse.jgit.lib.ObjectId;

import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GitAddTagReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GitAddTagReactor.class);
	private static final String COMMIT_ID_KEY = "commitId";

	public GitAddTagReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), COMMIT_ID_KEY,
				ReactorKeysEnum.TAGS.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String projectId = this.keyValue.get(this.keysToGet[0]);
		String commitId = this.keyValue.get(this.keysToGet[1]);
		String tag = this.keyValue.get(this.keysToGet[2]);

		if (projectId == null || (projectId = projectId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the project id");
		}
		if (tag == null || (tag = tag.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the tag");
		}
		if (commitId == null || (commitId = commitId.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the commit id");
		}

		if (!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have access to the project");
		}

		IProject project = Utility.getProject(projectId);
		String projectVersionFolder = EngineUtility.getSpecificEngineVersionFolder(IEngine.CATALOG_TYPE.PROJECT,
				projectId, project.getEngineName());

		try (Git thisGit = Git.open(new File(projectVersionFolder));) {
			ObjectId commitObjectId = thisGit.getRepository().resolve(commitId);
			thisGit.tag().setName(tag).setObjectId(thisGit.getRepository().parseCommit(commitObjectId)).call();
		} catch (RefAlreadyExistsException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Tag is already present " + tag, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred adding the tag. Detailed error = " + e.getMessage(), e);
		}

		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushProjectFolder(project, projectVersionFolder);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	public String getReactorDescription() {
		return "This reactor add tag to a particular commit id";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "This is a required field containing the project id of a project";
		} else if (key.equals(COMMIT_ID_KEY)) {
			return "This is a required field containing the commit id of a project";
		} else if (key.equals(ReactorKeysEnum.TAGS.getKey())) {
			return "This is a required field containing the tag of a project";
		}
		return super.getDescriptionForKey(key);
	}

}