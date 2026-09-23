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
package prerna.auth.utils.reactors.admin;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.cluster.sync.IClusterSynchronizer;
import prerna.cluster.sync.impl.ClusterSynchronizerFactory;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetSystemInfoReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if (adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		String hostname;
		try {
			hostname = System.getenv("hostname");
			if (hostname == null || hostname.isEmpty()) {
				hostname = java.net.InetAddress.getLocalHost().getHostName();
			}
		} catch (Exception e) {
			hostname = "unknown-host";
		}

		String ipaddress;
		try {
			ipaddress = java.net.InetAddress.getLocalHost().getHostAddress();
		} catch (Exception e) {
			ipaddress = "unknown-ipaddress";
		}

		Map<String, Object> systemInfoDetailsmap = new HashMap<>();
		systemInfoDetailsmap.put("hostname", hostname);
		systemInfoDetailsmap.put("ipaddress", ipaddress);
		systemInfoDetailsmap.put("isCluster", ClusterUtil.IS_CLUSTER);
		systemInfoDetailsmap.put("storageProvider", ClusterUtil.STORAGE_PROVIDER);
		systemInfoDetailsmap.put("isClusterScheduler", ClusterUtil.IS_CLUSTERED_SCHEDULER);
		systemInfoDetailsmap.put("clusterContainerIp", IClusterSynchronizer.CONTAINER_IP);
		systemInfoDetailsmap.put("isClusterSync", ClusterSynchronizerFactory.IS_CLUSTER_SYNC_SETUP);
		return new NounMetadata(systemInfoDetailsmap, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return """
					Admin only reactor returning a map with properties about the instance including:
					hostname, ipaddress, isCluster, storageProvider, isClusterScheduler, isClusterZK
				""";
	}

}
