package prerna.redis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Shared Redis pool factory for SEMOSS features that need Redis coordination.
 */
public final class RedisConnectionFactory {

	private static final ConcurrentMap<String, JedisPool> POOLS = new ConcurrentHashMap<>();

	private RedisConnectionFactory() {
	}

	public static JedisPool getPool() {
		return getPool(RedisConnectionConfig.fromDIHelper());
	}

	public static JedisPool getPool(RedisConnectionConfig config) {
		return POOLS.computeIfAbsent(config.cacheKey(), ignored -> createPool(config));
	}

	private static JedisPool createPool(RedisConnectionConfig config) {
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxTotal(config.getPoolMaxTotal());
		poolConfig.setMaxIdle(config.getPoolMaxIdle());
		poolConfig.setMinIdle(config.getPoolMinIdle());
		poolConfig.setTestOnBorrow(true);
		poolConfig.setTestWhileIdle(true);
		String password = config.getPassword();
		if (password != null && !password.trim().isEmpty()) {
			return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs(), password);
		}
		return new JedisPool(poolConfig, config.getHost(), config.getPort(), config.getTimeoutMs());
	}
}
