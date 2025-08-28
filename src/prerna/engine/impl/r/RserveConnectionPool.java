/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.r;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import prerna.util.PortAllocator;

public class RserveConnectionPool implements IRserveConnectionPool {

	// TODO >>>timb: R - since this is running locally, do we need to specify the
	// host at all? Can we
	// just return a port for get connection? (later)
	private static final String HOST = "127.0.0.1";

	private Map<RserveConnectionMeta, Integer> pool = new ConcurrentHashMap<>();

	//////////////////////////////////////////////////////////////////////
	// Singleton implementation of IRserveConnectionPool
	//////////////////////////////////////////////////////////////////////
	private static class LazyHolder {
		private static final RserveConnectionPool INSTANCE = new RserveConnectionPool();
	}

	public static RserveConnectionPool getInstance() {
		return LazyHolder.INSTANCE;
	}

	private RserveConnectionPool() {
	}

	//////////////////////////////////////////////////////////////////////
	// IRserveConnectionPool implementation
	//////////////////////////////////////////////////////////////////////
	@Override
	public RserveConnectionMeta getConnection() {

		// Start a new Rserve if the pool is still less than the max size
		if (pool.size() < RserveUtil.RSERVE_CONNECTION_POOL_SIZE) {
			int port = PortAllocator.getInstance().getNextAvailablePort();
			try {
				Process p = RserveUtil.startR(port);
				RserveConnectionMeta connection = new RserveConnectionMeta(HOST, port);
				connection.setProcess(p);
				pool.put(connection, 1);
				return connection;
			} catch (Exception e) {
				throw new IllegalArgumentException("Failed to get connection.", e);
			}
		} else {
			// Otherwise, grab the least connection and increment by one
			Map<RserveConnectionMeta, Integer> leastConnection = pool.entrySet().stream()
					.sorted(Map.Entry.comparingByValue()).limit(1)
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			RserveConnectionMeta connection = leastConnection.keySet().iterator().next();
			pool.compute(connection, (k, v) -> v + 1);
			return connection;
		}
	}

	@Override
	public void releaseConnection(RserveConnectionMeta connection) {
		try {

			// If there is only one connection on the process, then also stop this Rserve
			// instance
			if (pool.remove(connection, 1)) {
				try {
					RserveUtil.stopR(connection.getPort());
				} catch (Exception e) {
					throw new IllegalArgumentException("Failed to release connection.", e);
				}
			} else {

				// Otherwise decrement the connection by one
				pool.compute(connection, (k, v) -> v - 1);
			}
		} finally {
			connection.setRcon(null);
		}
	}

	@Override
	public void recoverConnection(RserveConnectionMeta connection) throws Exception {
		try {
			RserveUtil.stopR(connection.getPort());
			Process p = RserveUtil.startR(connection.getPort());
			connection.setProcess(p);
		} finally {
			connection.setRcon(null);
		}
	}

	@Override
	public void shutdown() throws Exception {
		for (RserveConnectionMeta connection : pool.keySet()) {
			try {
				RserveUtil.stopR(connection.getPort());
			} catch (Exception ignore) {
				// Ignore
			} finally {
				pool.remove(connection);
			}
		}
	}
}
