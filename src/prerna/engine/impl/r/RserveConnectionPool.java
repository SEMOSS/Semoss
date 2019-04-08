package prerna.engine.impl.r;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import prerna.cluster.util.ClusterUtil;

public class RserveConnectionPool implements IRserveConnectionPool {

	// TODO >>>timb: R - since this is running locally, do we need to specify the host at all? Can we just return a port for get connection? (later)
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
		if (pool.size() < ClusterUtil.RSERVE_CONNECTION_POOL_SIZE) {
			int port = RserveUtil.getOpenPort();
			try {
				RserveUtil.startR(port);
				RserveConnectionMeta connection = new RserveConnectionMeta(HOST, port);
				pool.put(connection, 1);
				return connection;
			} catch (Exception e) {
				throw new IllegalArgumentException("Failed to get connection.", e);
			}
		} else {
			
			// Otherwise, grab the least connection and increment by one
			Map<RserveConnectionMeta, Integer> leastConnection = pool.entrySet().stream()
					.sorted(Map.Entry.comparingByValue())
					.limit(1)
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
			RserveConnectionMeta connection = leastConnection.keySet().iterator().next();
			pool.compute(connection, (k, v) -> v + 1);
			return connection;
		}
	}

	@Override
	public void releaseConnection(RserveConnectionMeta connection) {
		
		// If there is only one connection on the process, then also stop this Rserve instance
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
		
	}

	@Override
	public void recoverConnection(RserveConnectionMeta connection) throws Exception {
		RserveUtil.stopR(connection.getPort());
		connection.setRcon(null); // Now any connection is definitely invalid
		RserveUtil.startR(connection.getPort());
	}

	@Override
	public void shutdown() throws Exception {
		IRUserConnection.endR();
		pool.clear();
	}

}
