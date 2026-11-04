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
package prerna.cluster.util;

import java.util.HashMap;
import java.util.Map;

import prerna.util.EngineSyncUtility;
import prerna.util.ProjectSyncUtility;

/**
 * Type-safe catalog of the actions that one cluster node can ask its peers to
 * run in response to a change published through an
 * {@link prerna.cluster.sync.IClusterSynchronizer}. Most are
 * {@link ClusterUtil} "pull" actions that refresh a local copy from cloud
 * storage; the rest act on what a peer has open, such as closing an engine or
 * clearing a project's python imports.
 * <p>
 * Each constant maps a stable {@code wireName} (the token published across
 * nodes by the active synchronizer) to a direct, compiled call of the
 * corresponding method. Because {@link #invoke(Object[])} calls the real method
 * rather than resolving it reflectively by name, renaming or changing the
 * signature of a target method becomes a compile-time error here instead of a
 * runtime failure on a peer node.
 * <p>
 * The {@code wireName} values intentionally match the historical method-name
 * strings so the payloads stay compatible with nodes running older builds.
 */
public enum ClusterSyncMethod {

	/**
	 * Pulls an engine's folder and smss file from cloud storage so a peer that has
	 * the engine replaces its local copy with the one just pushed, such as after an
	 * smss edit. A peer that has the engine open closes it before replacing its
	 * files and opens it again from them, so the running engine and its python
	 * process pick up the change. Params: the engine id.
	 */
	PULL_ENGINE("pullEngine") {
		@Override
		public void invoke(Object[] params) {
			String engineId = (String) params[0];
			ClusterUtil.pullEngine(engineId, null, EngineSyncUtility.isEngineOpen(engineId));
		}
	},
	/**
	 * Pulls one folder of an engine from cloud storage, such as a folder of files
	 * edited in the engine's assets, without pulling the rest of the engine.
	 * Params: the engine id, the folder's absolute path on the node, and its path
	 * in cloud storage relative to the engine.
	 */
	PULL_ENGINE_FOLDER("pullEngineFolder") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullEngineFolder((String) params[0], (String) params[1], (String) params[2]);
		}
	},
	/**
	 * Pulls a database's OWL file from cloud storage after its metadata changed, so
	 * a peer reads the current tables, columns and relationships. Params: the
	 * database id.
	 */
	PULL_OWL("pullOwl") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullOwl((String) params[0]);
		}
	},
	/**
	 * Pulls a project's folder and smss file from cloud storage so a peer that has
	 * the project replaces its local copy with the one just pushed, such as after
	 * an smss edit. A peer that has the project open closes it before replacing its
	 * files and opens it again from them. Params: the project id.
	 */
	PULL_PROJECT("pullProject") {
		@Override
		public void invoke(Object[] params) {
			String projectId = (String) params[0];
			ClusterUtil.pullProject(projectId, ProjectSyncUtility.isProjectOpen(projectId));
		}
	},
	/**
	 * Pulls one folder of a project from cloud storage, such as a folder of app
	 * files that was saved, without pulling the rest of the project. Params: the
	 * project id, the folder's absolute path on the node, and its path in cloud
	 * storage relative to the project.
	 */
	PULL_PROJECT_FOLDER("pullProjectFolder") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullProjectFolder((String) params[0], (String) params[1], (String) params[2]);
		}
	},
	/**
	 * Pulls one insight's folder of a project from cloud storage after the insight
	 * was saved. Params: the project id and the insight id.
	 */
	PULL_INSIGHT("pullInsight") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullInsight((String) params[0], (String) params[1]);
		}
	},
	/**
	 * Pulls a project's insights database from cloud storage after an insight was
	 * added, changed or removed, so a peer lists the current insights. Params: the
	 * project id.
	 */
	PULL_INSIGHTS_DB("pullInsightsDB") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullInsightsDB((String) params[0]);
		}
	},
	/**
	 * Pulls a user's asset project from cloud storage after the user's own files
	 * changed, on a peer that already has a copy of them. Params: the id of the
	 * user's asset project.
	 */
	PULL_USER_ASSET("pullUserAsset") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullUserAsset((String) params[0]);
		}
	},
	/**
	 * Closes the engine where a peer has it open, so the peer opens it again from
	 * its current files on next use. This is how a restart of an engine reaches
	 * every node. Params: the engine id.
	 */
	CLOSE_ENGINE("closeEngine") {
		@Override
		public void invoke(Object[] params) {
			EngineSyncUtility.closeLoadedEngine((String) params[0]);
		}
	},
	/**
	 * Clears the project modules cached in the project python process where a peer
	 * has it running, so the next import reads the current .py files. Params: the
	 * project id.
	 */
	RELOAD_PROJECT("reloadProject") {
		@Override
		public void invoke(Object[] params) {
			ProjectSyncUtility.clearLoadedProjectPythonImports((String) params[0]);
		}
	};

	/**
	 * The token published across nodes to name this action. It never changes, so
	 * nodes on older and newer builds read each other's events.
	 */
	private final String wireName;

	/**
	 * Every action keyed by its wire token, for reading a published event back.
	 */
	private static final Map<String, ClusterSyncMethod> BY_WIRE_NAME = new HashMap<>();
	static {
		for (ClusterSyncMethod method : values()) {
			BY_WIRE_NAME.put(method.wireName, method);
		}
	}

	/**
	 * Creates the action published under the given token.
	 *
	 * @param wireName the token published across nodes to name this action
	 */
	ClusterSyncMethod(String wireName) {
		this.wireName = wireName;
	}

	/**
	 * Returns the token published across nodes to name this action, which
	 * {@link #fromWireName(String)} reads back on the receiving node.
	 *
	 * @return the stable token published across nodes to identify this action
	 */
	public String getWireName() {
		return this.wireName;
	}

	/**
	 * Executes the action represented by this constant.
	 *
	 * @param params deserialized arguments, in the same order they were published
	 */
	public abstract void invoke(Object[] params);

	/**
	 * Resolves the action published under the given wire token.
	 *
	 * @param wireName token read back from a published change event
	 * @return the matching {@link ClusterSyncMethod}, or {@code null} if the token
	 *         is not recognized (e.g. published by a newer build)
	 */
	public static ClusterSyncMethod fromWireName(String wireName) {
		return BY_WIRE_NAME.get(wireName);
	}
}
