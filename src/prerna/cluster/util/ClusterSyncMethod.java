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

/**
 * Type-safe catalog of the {@link ClusterUtil} "pull" actions that one cluster
 * node can ask its peers to run in response to a change published through an
 * {@link prerna.cluster.sync.IClusterSynchronizer}.
 * <p>
 * Each constant maps a stable {@code wireName} (the token published across
 * nodes by the active synchronizer) to a direct, compiled call of the
 * corresponding {@link ClusterUtil} method. Because {@link #invoke(Object[])}
 * calls the real method rather than resolving it reflectively by name, renaming
 * or changing the signature of a target method becomes a compile-time error
 * here instead of a runtime failure on a peer node.
 * <p>
 * The {@code wireName} values intentionally match the historical method-name
 * strings so the payloads stay compatible with nodes running older builds.
 */
public enum ClusterSyncMethod {

	PULL_ENGINE("pullEngine") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullEngine((String) params[0]);
		}
	},
	PULL_ENGINE_FOLDER("pullEngineFolder") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullEngineFolder((String) params[0], (String) params[1], (String) params[2]);
		}
	},
	PULL_OWL("pullOwl") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullOwl((String) params[0]);
		}
	},
	PULL_PROJECT("pullProject") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullProject((String) params[0]);
		}
	},
	PULL_PROJECT_FOLDER("pullProjectFolder") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullProjectFolder((String) params[0], (String) params[1], (String) params[2]);
		}
	},
	PULL_INSIGHT("pullInsight") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullInsight((String) params[0], (String) params[1]);
		}
	},
	PULL_INSIGHTS_DB("pullInsightsDB") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullInsightsDB((String) params[0]);
		}
	},
	PULL_USER_ASSET("pullUserAsset") {
		@Override
		public void invoke(Object[] params) {
			ClusterUtil.pullUserAsset((String) params[0]);
		}
	};

	private final String wireName;

	private static final Map<String, ClusterSyncMethod> BY_WIRE_NAME = new HashMap<>();
	static {
		for (ClusterSyncMethod method : values()) {
			BY_WIRE_NAME.put(method.wireName, method);
		}
	}

	ClusterSyncMethod(String wireName) {
		this.wireName = wireName;
	}

	/**
	 * @return the stable token published across nodes to identify this action
	 */
	public String getWireName() {
		return this.wireName;
	}

	/**
	 * Executes the {@link ClusterUtil} action represented by this constant.
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
