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
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent;

import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.util.AssetUtility;

/**
 * The authorized filesystem target for one agent run.
 *
 * <p>The target is resolved before a harness starts and retained for lifecycle
 * hooks and final persistence, so those paths do not reinterpret {@code space}.
 * {@code rootDirectory} is the authorized filesystem boundary.
 * {@code workingDirectory} is either that root or a validated descendant when
 * the run specifies a {@code subdir}.
 */
public final class AgentRunTarget {

	private final String space;
	private final IProject project;
	/** Authorized filesystem boundary; an agent path must remain under this root. */
	private final String rootDirectory;
	/** Root directory itself, or {@code rootDirectory/subdir} after validation. */
	private final String workingDirectory;
	private final String gitFolder;

	private AgentRunTarget(String space, IProject project, String rootDirectory,
			String workingDirectory, String gitFolder) {
		this.space = space;
		this.project = project;
		this.rootDirectory = rootDirectory;
		this.workingDirectory = workingDirectory;
		this.gitFolder = gitFolder;
	}

	public static AgentRunTarget insight(String rootDirectory) {
		return new AgentRunTarget(AssetUtility.INSIGHT_SPACE_KEY, null, rootDirectory, rootDirectory, null);
	}

	public static AgentRunTarget inherited(String workingDirectory) {
		return new AgentRunTarget(AssetUtility.INSIGHT_SPACE_KEY, null, workingDirectory, workingDirectory, null);
	}

	public static AgentRunTarget user(IProject project, String rootDirectory, String gitFolder) {
		return new AgentRunTarget(AssetUtility.USER_SPACE_KEY, project, rootDirectory, rootDirectory, gitFolder);
	}

	public static AgentRunTarget project(IProject project, String rootDirectory, String gitFolder) {
		return new AgentRunTarget(project.getProjectId(), project, rootDirectory, rootDirectory, gitFolder);
	}

	public AgentRunTarget withWorkingDirectory(String workingDirectory) {
		return new AgentRunTarget(space, project, rootDirectory, workingDirectory, gitFolder);
	}

	/**
	 * Returns the asset-space selector that can be re-authorized for a child run.
	 * This is {@code USER} for user assets, a project id for project assets, and
	 * {@code INSIGHT} for room-local targets.
	 */
	public String getSpace() {
		return space;
	}

	/**
	 * Returns the working directory relative to this target's authorized root.
	 * A root-level working directory is represented by {@code "."} so a child
	 * run does not apply its workspace's default subdir instead.
	 */
	public String getWorkingSubdir() {
		try {
			java.nio.file.Path root = new java.io.File(rootDirectory).getCanonicalFile().toPath();
			java.nio.file.Path working = new java.io.File(workingDirectory).getCanonicalFile().toPath();
			if (!working.startsWith(root)) {
				throw new IllegalStateException("Agent working directory is outside its authorized root");
			}
			java.nio.file.Path relative = root.relativize(working);
			return relative.getNameCount() == 0 ? "." : relative.toString();
		} catch (java.io.IOException | java.nio.file.InvalidPathException e) {
			throw new IllegalStateException("Agent target contains an invalid filesystem path", e);
		}
	}

	public String getProjectId() {
		return project == null ? null : project.getProjectId();
	}

	/** Returns the authorization boundary used to validate an optional subdir. */
	String getRootDirectory() {
		return rootDirectory;
	}

	/**
	 * Returns the directory exposed to the agent. This equals
	 * {@link #getRootDirectory()} unless a validated {@code subdir} was applied.
	 */
	public String getWorkingDirectory() {
		return workingDirectory;
	}

	public String getGitFolder() {
		return gitFolder;
	}

	public boolean isInsight() {
		return AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(space);
	}

	public boolean isUser() {
		return AssetUtility.USER_SPACE_KEY.equalsIgnoreCase(space);
	}

	public boolean isProject() {
		return project != null && !isUser();
	}

	/**
	 * Push the selected asset target to central storage when clustering is active.
	 * Room messages are still synchronized separately by the runner/tool handlers.
	 */
	public void pushToCluster() {
		if (!ClusterUtil.IS_CLUSTER) {
			return;
		}
		if (isUser()) {
			ClusterUtil.pushUserAsset(getProjectId());
		} else if (isProject() && project != null) {
			ClusterUtil.pushProjectFolder(project, workingDirectory);
		}
	}

	@Override
	public String toString() {
		return "AgentRunTarget{" + "space='" + space + '\'' + ", projectId='" + getProjectId()
				+ '\'' + ", rootDirectory='" + rootDirectory + '\'' + ", workingDirectory='" + workingDirectory
				+ '\'' + ", gitFolder='" + gitFolder + '\'' + '}';
	}
}
