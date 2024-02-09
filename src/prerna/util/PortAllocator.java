package prerna.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PortAllocator {

	private static final Logger classLogger = LogManager.getLogger(PortAllocator.class);

	private static PortAllocator instance;
	private final int MIN_PORT;
	private final int MAX_PORT;
	private final int PORT_DOMAIN;
	private AtomicInteger nextPort;

	/**
	 * 
	 */
	private PortAllocator() {
		int lowPort = 5355;
		int highPort = lowPort + 10_000;

		if (DIHelper.getInstance().getProperty("LOW_PORT") != null) {
			try {
				lowPort = Integer.parseInt(DIHelper.getInstance().getProperty("LOW_PORT"));
			} catch (Exception ignore) {
			}
			;
		}
		if (DIHelper.getInstance().getProperty("HIGH_PORT") != null) {
			try {
				highPort = Integer.parseInt(DIHelper.getInstance().getProperty("HIGH_PORT"));
			} catch (Exception ignore) {
			}
			;
		}

		MIN_PORT = lowPort;
		MAX_PORT = highPort;
		PORT_DOMAIN = highPort - lowPort;
		nextPort = new AtomicInteger(MIN_PORT);
	}

	/**
	 * 
	 * @return
	 */
	public static PortAllocator getInstance() {
		if (instance == null) {
			synchronized (PortAllocator.class) {
				if (instance == null) {
					instance = new PortAllocator();
				}
			}
		}
		return instance;
	}

	/**
	 * 
	 * @return
	 */
	public int getNextAvailablePort() {
		int port;
		int counter = 0;
		while (true) {
			port = nextPort.getAndIncrement();
			if (port > MAX_PORT) {
				nextPort.set(MIN_PORT);
				port = MIN_PORT;
			}
			if (isPortAvailable(port)) {
				break;
			}
			
			// make sure we don't have an infinite loop
			counter++;
			if(counter > PORT_DOMAIN) {
				throw new IllegalArgumentException("Unable to find an open port");
			}
		}
		return port;
	}

	/**
	 * 
	 * @param port
	 * @return
	 */
	private boolean isPortAvailable(int port) {
		try (ServerSocket ignored = new ServerSocket(port)) {
			classLogger.info("Port " + port + " is available");
			return true;
		} catch (IOException e) {
			classLogger.info("Port " + port + " is unavailable");
			return false;
		}
	}
}
