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
package prerna.cluster.util.clients;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import prerna.cluster.util.ClusterSyncMethod;
import prerna.engine.impl.pipeline.PlatformOperation;

/**
 * A cloud storage client whose every call runs as a platform operation. Syncing
 * the files of an engine or a project with the cluster's cloud storage is the
 * platform's own work, whoever's request set it off and on a thread with no
 * user: the sync closes the engine, moves its files and opens it again, and the
 * engine proxy lets those calls through inside the operation.
 */
public final class ClusterSyncCloudClient implements InvocationHandler {

	private final ICloudClient client;

	private ClusterSyncCloudClient(ICloudClient client) {
		this.client = client;
	}

	/**
	 * Wraps a cloud storage client so each of its calls is a platform operation.
	 *
	 * @param client the client that talks to cloud storage
	 * @return a client that passes every call on to it inside a platform operation
	 */
	public static ICloudClient of(ICloudClient client) {
		return ICloudClient.class.cast(Proxy.newProxyInstance(ICloudClient.class.getClassLoader(),
				new Class<?>[] { ICloudClient.class }, new ClusterSyncCloudClient(client)));
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		try (PlatformOperation operation = PlatformOperation.begin(ClusterSyncMethod.CLUSTER_SYNC_OPERATION)) {
			return method.invoke(this.client, args);
		} catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

}
