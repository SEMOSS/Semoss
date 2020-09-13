package prerna.rpa.reporting;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import prerna.rpa.db.jedis.JedisStore;
import redis.clients.jedis.Jedis;

/**
 * Centralized handler for {@link AbstractReportProcess}' that encounter a
 * {@code JedisDataException} while processing. Runs in its own thread.
 * 
 * @author tbanach
 *
 */
public class JedisDataExceptionHandler implements Runnable {

	private static final Logger LOGGER = LogManager.getLogger(JedisDataExceptionHandler.class.getName());
	
	private static final String INTERRUPTION_MESSAGE = "Thread for the Jedis data exception handler interrupted in an unexpected manner.";
	
	private final Object handlingMonitor = new Object();
	private final Object releasedMonitor = new Object();

	private volatile boolean currentlyHandling = false;
	private volatile boolean released = false;
	
	@Override
	public void run() {
		Configurator.setLevel(LOGGER.getName(), Level.DEBUG);
		synchronized (releasedMonitor) {
			try {
				while (!released) {
					releasedMonitor.wait();
					LOGGER.debug("Released Jedis data exception handler.");
				}
			} catch (InterruptedException e) {
				LOGGER.error(INTERRUPTION_MESSAGE, e);

				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
		}
	}
	
	/**
	 * Bails processes out of {@code JedisDataException}s that occur when a Redis
	 * {@code BGSAVE} fails, locking further writes. Does this by temporarily
	 * setting {@code stop-writes-on-bgsave-error} to {@code no} and performing a
	 * {@code SAVE}, which, unlike {@code BGSAVE} holds until completion. Once the save
	 * is completed, {@code stop-writes-on-bgsave-error} is restored to {@code yes},
	 * and all waiting threads are notified. Only one thread will perform this
	 * {@code SAVE}; all others will wait for this first thread's notification of
	 * completion.
	 */
	public void handleJedisDataException() {
		if (startHandling()) {
			
			// Temporarily allow writes despite the background save error and manually save
			LOGGER.debug(">>>Handling Jedis data exception.");
			currentlyHandling = true;
			try (Jedis jedis = JedisStore.getInstance().getResource()) {
				jedis.configSet("stop-writes-on-bgsave-error", "no");
				String status = jedis.save();
				LOGGER.debug(">>>Saved Jedis data with a status of: " + status);
				jedis.configSet("stop-writes-on-bgsave-error", "yes");
			}
			currentlyHandling = false;
			LOGGER.debug(">>>Finished handling Jedis data exception.");
			
			// Notify waiting threads that they can continue execution
			LOGGER.debug(">>>Notifying all waiting threads.");
			synchronized (handlingMonitor) {
				handlingMonitor.notifyAll();
			}
		} else {
			
			// Wait for the exception to be handled by another thread
			LOGGER.debug(">>>Another thread is already handling the Jedis data exception; waiting for it to finish.");
			try {
				synchronized (handlingMonitor) {
					handlingMonitor.wait();
				}
			} catch (InterruptedException e) {
				LOGGER.error(INTERRUPTION_MESSAGE, e);

				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
			LOGGER.debug(">>>Was notified of completion.");
		}
		LOGGER.debug(">>>Jedis data exception has been handled; continuing execution.");
	}
		
	// Determine whether to start handling
	private synchronized boolean startHandling() {
		boolean startHandling;
		if (currentlyHandling) {
			startHandling = false;
		} else {
			startHandling = true;
			currentlyHandling = true;
		}
		return startHandling;
	}
	
	/**
	 * Once processing is complete, release this handler thread so it can terminate.
	 */
	public void release() {
		released = true;
		synchronized (releasedMonitor) {
			releasedMonitor.notifyAll();
		}
	}
		
}
