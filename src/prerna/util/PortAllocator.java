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
package prerna.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class PortAllocator {

	private static final Logger classLogger = LogManager.getLogger(PortAllocator.class);

	private static volatile PortAllocator instance;
	private final int MIN_PORT;
	private final int MAX_PORT;
	private final int PORT_DOMAIN;
	private static AtomicInteger nextPort;

	/**
	 * 
	 */
	private PortAllocator() {
		int lowPort = 5355;
		int highPort = lowPort + 10_000;

		if (Utility.getDIHelperProperty("LOW_PORT") != null) {
			try {
				lowPort = Integer.parseInt(Utility.getDIHelperProperty("LOW_PORT"));
			} catch (Exception ignore) {
			}
		}
		if (Utility.getDIHelperProperty("HIGH_PORT") != null) {
			try {
				highPort = Integer.parseInt(Utility.getDIHelperProperty("HIGH_PORT"));
			} catch (Exception ignore) {
			}
		}

		MIN_PORT = lowPort;
		MAX_PORT = highPort;
		PORT_DOMAIN = highPort - lowPort;
		PortAllocator.nextPort = new AtomicInteger(MIN_PORT);
	}

	/**
	 * 
	 * @return
	 */
	public static PortAllocator getInstance() {
		if (instance != null) {
			return instance;
		}

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
			port = PortAllocator.nextPort.getAndIncrement();
			if (port > MAX_PORT) {
				// use compareAndSet so only 1 thread can change the value to MIN_PORT if there
				// is a race condition
				PortAllocator.nextPort.compareAndSet(port + 1, MIN_PORT);
				// let the loop re-run and pick up a valid port
				continue;
			}
			if (isPortAvailable(port)) {
				break;
			}

			// make sure we don't have an infinite loop
			counter++;
			if (counter > PORT_DOMAIN) {
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
	public static boolean isPortAvailable(int port) {
		try (ServerSocket ignored = new ServerSocket(port)) {
			classLogger.info("Port {} is available", port);
			return true;
		} catch (IOException e) {
			classLogger.info("Port {} is unavailable", port);
			return false;
		}
	}
}
