package prerna.rpa.db.jedis;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class JedisUtil {
	private static final Logger LOGGER = LogManager.getLogger(JedisUtil.class.getName());

	private static final String NEW_LINE = System.getProperty("line.separator");

	private static final String REDIS_HOST = "redis_host";
	private static final String REDIS_PORT = "redis_port";
	private static final String REDIS_TIMEOUT = "redis_timeout";
	private static final String REDIS_PASSWORD = "redis_password";
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private JedisUtil() {
		throw new IllegalStateException("Utility class");
	}
	
	public static JedisPool getJedisPool() {
		JedisPool jedisPool = null;
		try(InputStream input = new FileInputStream(Utility.getBaseFolder() + DIR_SEPARATOR + "config.properties")) {
			Properties prop = new Properties();

			prop.load(input);
			// JedisPool is thread safe
			String host = prop.getProperty(REDIS_HOST);
			int port = Integer.parseInt(prop.getProperty(REDIS_PORT));
			int timeout = Integer.parseInt(prop.getProperty(REDIS_TIMEOUT));
			String redisPassword = prop.getProperty(REDIS_PASSWORD);
			if (redisPassword.equals("null")) redisPassword = null;

			jedisPool = new JedisPool(new CustomJedisPoolConfig(), host, port, timeout, redisPassword);
		} catch (IOException ex) {
			LOGGER.error("Error with loading properties in config file" + ex.getMessage());
		}
		return jedisPool;
	}
	
	/**
	 * CAUTION: This method will "delete all the keys of all the existing databases" in Redis
	 */
	public static void deleteAllKeys() {
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			LOGGER.info("Deleting the keys: ");
			String keyQuery = "*";
			Set<String> keys = jedis.keys(keyQuery);
			for (String key : keys) {
				LOGGER.info(Utility.cleanLogString(key));
			}
			jedis.flushAll();
			int nKeysLeft = jedis.keys(keyQuery).size();
			LOGGER.info(keys.size() - nKeysLeft + "/" + keys.size() + " keys deleted.");
			if (nKeysLeft > 0) {
				LOGGER.warn("The following keys remain: ");
				logKeysForQuery(keyQuery);
			}
		}
	}
	
	/**
	 * CAUTION: This method will delete all the keys starting with the provided prefix in Redis
	 * @param prefix the substring at the beginning of Redis keys in a particular group
	 */
	public static void deleteAllKeysOfPrefix(String prefix) {
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			String keyQuery = prefix + "*";
			Set<String> keys = jedis.keys(keyQuery);
			StringBuilder keysString = new StringBuilder();
			for (String key : keys) {
				jedis.del(key);
				keysString.append(NEW_LINE).append(key);
			}
			LOGGER.info("Deleting the keys: " + Utility.cleanLogString(keysString.toString()));
			int nKeysLeft = jedis.keys(keyQuery).size();
			LOGGER.info(keys.size() - nKeysLeft + "/" + keys.size() + " keys for the prefix " + prefix + " deleted.");
			if (nKeysLeft > 0) {
				LOGGER.warn("The following keys remain: ");
				logKeysForQuery(keyQuery);
			}
		}
	}
	
	public static void logKeysForQuery(String keyQuery) {
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			for (String key : jedis.keys(keyQuery)) {
				LOGGER.info(Utility.cleanLogString(key));
			}
		}
	}
	
	public static void logSetMembers(String key) {
		StringBuilder membersString = new StringBuilder();
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			Set<String> members = jedis.smembers(key);
			membersString.append(NEW_LINE);
			membersString.append(key);
			membersString.append(":");
			membersString.append("{");
			int n = 0;
			for (String member : members) {
				n++;
				membersString.append(member);
				if (n != members.size()) {
					membersString.append(", ");
					if (n % 7 == 0) {
						membersString.append(NEW_LINE);
					}
				}
			}
			membersString.append("}");
		}
		LOGGER.info(Utility.cleanLogString(membersString.toString()));
	}
	
	public static void logHashMembers(String key) {
		StringBuilder membersString = new StringBuilder();
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			Map<String, String> map = jedis.hgetAll(key);
			membersString.append(NEW_LINE);
			membersString.append(key);
			membersString.append(":");
			membersString.append("{");
			membersString.append(NEW_LINE);
			int n = 0;
			for (Entry<String, String> entry : map.entrySet()) {
				n++;
				membersString.append(entry.getKey());
				membersString.append(": ");
				membersString.append(entry.getValue());
				if (n != map.size()) {
					membersString.append(NEW_LINE);
				}
			}
			membersString.append("}");
		}
		LOGGER.info(Utility.cleanLogString(membersString.toString()));
	}
	
}
