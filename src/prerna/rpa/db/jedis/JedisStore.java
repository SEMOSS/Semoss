package prerna.rpa.db.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisStore {

	private final JedisPool jedisPool;
	
	private volatile boolean destroyed = false;
	
	// Singleton JedisStore
	private JedisStore() {

		// JedisPool is thread safe
		jedisPool = JedisUtil.getJedisPool();
	}
    private static class LazyHolder {
    	
    	private LazyHolder() {
    		throw new IllegalStateException("Static class");
    	}
    	
        private static final JedisStore INSTANCE = new JedisStore();
    }
	
	/**
	 * Gets a singleton instance of the JedisStore, from which developers can call
	 * {@link #getResource} to get a Jedis resource. Developers should always use
	 * this method to get a Jedis instance when writing application code. However,
	 * this should not be used within JUnit tests, because destroying a JedisStore
	 * singleton in a test's teardown logic would render the JedisStore useless for
	 * any other test classes. Instead, use {@link JedisUtil#getJedisPool()} then
	 * call {@link JedisPool#destroy()} when tearing down the test.
	 * 
	 * @return JedisStore singleton instance
	 */
	public static JedisStore getInstance() {
		return LazyHolder.INSTANCE;
	}

	public Jedis getResource() {
		if (destroyed) {
			throw new IllegalStateException("The application has destroyed the Jedis pool. No resource available.");
		}
		return jedisPool.getResource();
	}
	
	/**
	 * Destroys the Jedis pool managed by the JedisStore. Developers should call
	 * this method when shutting down the application. Once destroyed,
	 * {@link #getResource()} will throw an {@code IllegalStateException} if called
	 * again. Therefore, the application will no longer be able to procure a Jedis
	 * resource once this method is called. For this reason, destroy should only be
	 * called during application shutdown.
	 */
	public void destroy() {
		destroyed = true;
		jedisPool.destroy();
	}
}
