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
package prerna.tcp;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationSource;

import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * TCP socket server responsible for accepting client connections and delegating
 * request processing to {@link SocketServerHandler}.
 */
public class SocketServer implements Runnable {

	// basically a process which works by looking for commands in TCP space
	private static final String CLASS_NAME = SocketServer.class.getName();
	private static final int ACCEPT_TIMEOUT_MS = 10_000;
	private static final int MAX_INITIAL_ACCEPT_ATTEMPTS = 6;
	private static final long CRASH_WAIT_TIMEOUT_MS = 10_000L;
	// allow multiple threads at the same time
	private static boolean multi = false;
	public static boolean testMode = false;

	private static Logger classLogger = null;

	// this is basically reference to the RDF Map
	private Properties prop = null;
	private String socketDir = null;

	private boolean done = false;
	private boolean crashSignaled = false;
	private int initialAcceptAttempts = 0;
	private boolean hasAcceptedInitialConnection = false;

	private Socket clientSocket = null;
	private ServerSocket serverSocket = null;

	private InputStream is = null;

	private SocketServerHandler ssh = new SocketServerHandler();

	public Object crash = new Object();

	/**
	 * Entry point used to bootstrap the socket server process.
	 *
	 * @param args runtime arguments where index {@code 0} is the socket working
	 *             directory, index {@code 1} is the RDF map path, and index
	 *             {@code 2} is the server port
	 * @throws Exception if startup dependencies cannot be initialized
	 */
	public static void main(String[] args) throws Exception {
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		// arg3 - port to start

		// create the watch service
		// start this thread

		// when event comes write it to the command
		// comment this for main execution
		// -Dlog4j.defaultInitOverride=TRUE

		if (args == null || args.length == 0) {
			args = new String[5];
			args[0] = "C:/workspace/Semoss/InsightCache/z1";
			args[1] = "C:/workspace/Semoss/RDF_Map.prop";
			;
			args[2] = "9999";
			args[3] = "r";
			args[4] = "mixed";
			multi = true;
			testMode = true;
		}

		if (args.length < 3) {
			throw new IllegalArgumentException(
					"Must pass in at least 3 inputs - the log4j file, the rdf file map, and the port to run the socket on");
		}

		// this socket dir should have the log4j file contianer inside it
		String socketDir = args[0];
		String rdfMapInput = args[1];
		String portInput = args[2];

		String log4JPropFile = Paths.get(Utility.normalizePath(socketDir), "log4j2.properties").toAbsolutePath()
				.toString();

		// set to say this is not core
		DIHelper.getInstance().setLocalProperty("core", "false");

		classLogger = LogManager.getLogger(CLASS_NAME);
		try (FileInputStream fis = new FileInputStream(Utility.normalizePath(log4JPropFile))) {
			new ConfigurationSource(fis);
		} catch (IOException e) {
			classLogger.error("Failed to load log4j2 properties from {}", log4JPropFile, e);
		}

		int port = -1;
		try {
			port = Integer.parseInt(portInput);
		} catch (NumberFormatException e) {
			classLogger.error("Invalid socket server port input: {}", portInput, e);
			throw new IllegalArgumentException("Input integer input for port='" + portInput + "'");
		}

		String rdfMapLocation = Utility.normalizePath(rdfMapInput);
		Properties rdfMap = Utility.loadProperties(rdfMapLocation);
		classLogger.info("Loaded rdf map");

		DIHelper.getInstance().loadCoreProp(rdfMapLocation);
		DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);

		SocketServer worker = new SocketServer();
		worker.prop = rdfMap;
		worker.socketDir = socketDir;
		String engine = "r";
		if (args.length >= 4) {
			engine = args[3];
		}
		if (args.length >= 5) {
			SocketServer.multi = args[4].equalsIgnoreCase("multi");
		}

		worker.bootServer(port, engine);
	}

	/**
	 * Binds and starts the socket listener thread.
	 *
	 * @param PORT   socket port to bind
	 * @param engine engine identifier kept for startup compatibility
	 */
	public void bootServer(final int PORT, String engine) {
		try {
			serverSocket = new ServerSocket(PORT);
			// Avoid blocking forever on initial client connect attempts.
			serverSocket.setSoTimeout(ACCEPT_TIMEOUT_MS);
		} catch (IOException e) {
			classLogger.error("Could not listen on port {}", PORT, e);
			System.exit(1);
		}
		classLogger.info("server started");

		Thread listenerThread = new Thread(this);
		listenerThread.start();
	}

	/**
	 * Continuously accepts client connections and starts a handler thread for each
	 * accepted socket based on the server's threading mode.
	 */
	@Override
	public void run() {
		// do the listening here and then spawn the thread
		while (!done) {
			if (this.clientSocket == null || multi) {
				try {
					clientSocket = serverSocket.accept();
					if (!hasAcceptedInitialConnection) {
						hasAcceptedInitialConnection = true;
						initialAcceptAttempts = 0;
						classLogger.info("Accepted initial socket connection");
					}
				} catch (SocketTimeoutException e) {
					if (!hasAcceptedInitialConnection) {
						initialAcceptAttempts++;
						classLogger.warn("No socket client connected within {} ms (attempt {}/{})", ACCEPT_TIMEOUT_MS,
								initialAcceptAttempts, MAX_INITIAL_ACCEPT_ATTEMPTS);
						if (initialAcceptAttempts >= MAX_INITIAL_ACCEPT_ATTEMPTS) {
							classLogger.error(
									"Failed to establish initial socket client connection after {} timed attempts",
									MAX_INITIAL_ACCEPT_ATTEMPTS);
							done = true;
							break;
						}
					}
					continue;
					} catch (IOException e) {
						classLogger.error("Socket accept failed on listening port {}; shutting down server loop",
								serverSocket != null ? serverSocket.getLocalPort() : "unknown", e);
						System.exit(1);
					}
					try {
					// One handler instance is bound to one accepted client connection.
					ssh = new SocketServerHandler();
					DIHelper.getInstance().setLocalProperty("SSH", ssh);
						ssh.setLogger(classLogger);
						ssh.setOutputStream(clientSocket.getOutputStream());
						is = clientSocket.getInputStream();
					} catch (IOException e) {
						classLogger.error(
								"Unable to initialize socket streams for client {}. Closing current client connection.",
								clientSocket != null ? clientSocket.getRemoteSocketAddress() : "unknown", e);
						closeStream(clientSocket);
						clientSocket = null;
						continue;
					}

				// Wire server->handler context so the handler can callback into
				// signalCrash() when its socket loop fails.
				ssh.is = is;
				ssh.socket = serverSocket;
				ssh.server = this;
				ssh.mainFolder = socketDir;

				// Handler owns the socket read loop for this client.
				Thread readerThread = new Thread(ssh);
				readerThread.start();
			} else {
				// just sleep
				// See if the active handler crashed; handler calls signalCrash().
				synchronized (crash) {
					try {
						while (!crashSignaled) {
							crash.wait(CRASH_WAIT_TIMEOUT_MS);
						}
						crashSignaled = false;
						closeStream(clientSocket);
						clientSocket = null;
						closeStream(serverSocket);
						closeStream(is);
						is = null;
						if (!testMode) {
							ssh.cleanUp();
						}
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							done = true;
							classLogger.error(
									"Server listener thread interrupted while waiting up to {} ms for crash signal",
									CRASH_WAIT_TIMEOUT_MS, e);
						}
					}
				}
		}
	}

	/**
	 * Signals the server listener thread that a handler crash was detected.
	 */
	public void signalCrash() {
		synchronized (crash) {
			crashSignaled = true;
			crash.notifyAll();
		}
	}

	/**
	 * Closes a closeable resource while safely handling null references and close
	 * failures.
	 *
	 * @param closeThis resource to close
	 */
	private void closeStream(Closeable closeThis) {
		if (closeThis == null) {
			return;
		}
		try {
			closeThis.close();
		} catch (IOException e) {
			classLogger.error("Failed to close resource of type {}", closeThis.getClass().getName(), e);
		}
	}

	/**
	 * Indicates whether the server is configured to handle multiple connections in
	 * parallel.
	 *
	 * @return {@code true} when running in multi-threaded mode
	 */
	public static boolean isMulti() {
		return multi;
	}
}
