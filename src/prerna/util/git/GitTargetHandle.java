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

import prerna.cluster.util.ClusterUtil;

/**
 * A resolved engine or project: its id, the version folder holding the git
 * working tree, and the cluster push that persists that folder after a
 * mutation. Produced by {@link GitReactorTarget#resolve(String)} so reactor
 * bodies never need to know which flavor they are running against.
 */
public final class GitTargetHandle {

	private final String id;
	private final String versionFolder;
	private final Runnable pushClusterLambda;

	public GitTargetHandle(String id, String versionFolder, Runnable pushClusterLambda) {
		this.id = id;
		this.versionFolder = versionFolder;
		this.pushClusterLambda = pushClusterLambda;
	}

	public String getId() {
		return this.id;
	}

	public String getVersionFolder() {
		return this.versionFolder;
	}

	/**
	 * Pushes the version folder to the cluster store when running in a cluster, and
	 * does nothing otherwise. Mutating reactors call this after the git write and
	 * before they build their response.
	 */
	public void pushToCluster() {
		if (ClusterUtil.IS_CLUSTER) {
			Thread.startVirtualThread(this.pushClusterLambda);
		}
	}
}
