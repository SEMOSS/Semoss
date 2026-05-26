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
package prerna.engine.impl.r;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Utility;

public class RSingleton {

	private static final Logger classLogger = LogManager.getLogger(RSingleton.class);

	private static RConnection rcon = null;

	public static final String R_HOME = "R_HOME";
	public static final String R_LIBS = "R_LIBS";
	public static final String RSERVE_LOC = "/Rserve/libs/x64/Rserve";

	static int port = -1;
	static Hashtable<Integer, RConnection> portToCon = new Hashtable<Integer, RConnection>(); // RServe is not allowing
																								// me to inspect the
																								// port so I have to do
																								// this.. sorry

	private RSingleton() {
		port = PortAllocator.getInstance().getNextAvailablePort();
	}

	/**
	 * Get the connection and build if doesn't exist
	 * 
	 * @return
	 */
	public static RConnection getConnection() {
		if (rcon == null) {
			int port = PortAllocator.getInstance().getNextAvailablePort();
			return getConnection("127.0.0.1", port);
		}
		return rcon;
	}

	public static RConnection getConnection(int port) {
		return getConnection("127.0.0.1", port);
	}

	/**
	 * Get the rconn object as is. This maybe null
	 * 
	 * @return
	 */
	public static RConnection getRCon() {
		return rcon;
	}

	public static void startRServe(int port) {
		try {
			String rHome = System.getenv(R_HOME);
			String rLibs = System.getenv(R_LIBS);
			classLogger.info("R_HOME env is is " + rHome);
			classLogger.info("R_LIBS env is is " + rLibs);

			// If R_HOME / R_LIBS doesn't exist, then check RDF_Map
			if (rHome == null || rHome.isEmpty()) {
				rHome = Utility.getDIHelperProperty(R_HOME);
			}
			if (rLibs == null || rLibs.isEmpty()) {
				rLibs = Utility.getDIHelperProperty(R_LIBS);
			}

			// we like to keep our paths unix based
			rHome = rHome.replace("\\", "/");

			Path rHomePath = Paths.get(rHome);
			if (!Files.isDirectory(rHomePath)) {
				throw new IllegalArgumentException("rHome does not exist or is not a directory");
			}
			String rExe = rHome + "/bin/R";

			// if we dont have a rLibs, lets assume its in rhome
			if (rLibs == null) {
				rLibs = rHome + "/library";
			} else {
				rLibs = rLibs.replace("\\", "/");
			}

			classLogger.info("R_HOME for the process is " + rHome);
			classLogger.info("R_EXE for the process is " + rExe);

			ProcessBuilder pb;
			if (SystemUtils.IS_OS_WINDOWS) {
				pb = new ProcessBuilder(rExe, "CMD", rLibs + RSERVE_LOC, "--vanilla", "--RS-port", port + "");
			} else {
				pb = new ProcessBuilder(rExe, "CMD", "Rserve", "--vanilla", "--RS-port", port + "");
			}

			Process process = pb.start();

			classLogger.info("Waiting 1 second to allow Rserve to finish starting up...");
			try {
				process.waitFor(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			classLogger.info("Started RServe process");
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public static void stopRServe() {
		stopRServe(port);
	}

	public static void stopRServe(int port) {
		try {
			String rHome = System.getenv("R_HOME");
			if (rHome == null || rHome.isEmpty()) {
				rHome = Utility.getDIHelperProperty(R_HOME);
			}
			rHome = rHome.replace("\\", "/");

			Path rHomePath = Paths.get(rHome);
			if (!Files.isDirectory(rHomePath)) {
				throw new IllegalArgumentException("rHome does not exist or is not a directory");
			}
			rHome = rHome + "/bin/R";
			classLogger.info("R_HOME for process is " + rHome);

			ProcessBuilder pb = new ProcessBuilder("" + rHome + "", "-e",
					"library(Rserve);library(RSclient);rsc<-RSconnect(port=" + port + ");RSshutdown(rsc)", "--vanilla");
			Process process = pb.start();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	public static RConnection getConnection(String host, int port) {
		// this basically tries to do the same as get connection with a port
		if (portToCon.containsKey(port)) {
			rcon = portToCon.get(port);
		} else {
			classLogger.info("Making a master connection now on port " + port);
			try {
				rcon = new RConnection(host, port);
				portToCon.put(port, rcon);
			} catch (Exception ex) {
				classLogger.error(Constants.STACKTRACE, ex);
				// try to start again and see if that works
				startRServe(port);
				try {
					rcon = new RConnection(host, port);
					portToCon.put(port, rcon);
					RSingleton.port = port;
				} catch (RserveException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if (rcon == null) {
			classLogger.info("Generating master connection on port " + port + " is null");
		}

		return rcon;
	}

	/**
	 * KEEP THIS EVEN THOUGH NOT USED INCASE WE WNAT TO EVENTUALLY LOOK AT ALL THE
	 * PORTS IN PORT ALLOCATOR AND ATTEMPT TO USE THAT IF ITS AN RSERVE
	 * 
	 * @param port
	 * @return
	 */
	private static boolean isRServe(int port) {
		// try to see if this port is already running RServe
		boolean isRserve = false;
		classLogger.info("Trying to see if port " + port + " is already running Rserve.");
		try {
			rcon = new RConnection("127.0.0.1", port);
			portToCon.put(port, rcon);
			classLogger.info("Success! RServe: " + port);
			isRserve = true;
		} catch (Exception ex) {
			// Port isn't open, notify and move on
			classLogger.info("Port " + port + " is unavailable.");
		}

		return isRserve;
	}

}