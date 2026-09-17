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
package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.Server;

import prerna.engine.api.IEmbeddedRDBMSServerEngine;
import prerna.util.PortAllocator;
import prerna.util.Utility;

public class H2EmbeddedServerEngine extends RDBMSNativeEngine implements IEmbeddedRDBMSServerEngine {

	private static final Logger classLogger = LogManager.getLogger(H2EmbeddedServerEngine.class);
	private static final String DATABASE_RUNNING_ON = " DATABASE RUNNING ON ";

	private Server server;
	private String serverUrl;

	@Override
	protected String init(String connectionUrl, boolean force) {
		String baseConnUrl = connectionUrl;
		if (baseConnUrl.startsWith("jdbc:h2:nio:")) {
			baseConnUrl = baseConnUrl.substring("jdbc:h2:nio:".length());
		}
		if (force) {
			stopServer();
		}
		if (server == null) {
			try {
				// make sure the database file exists if it does not
				{
					File dbFile = new File(baseConnUrl + ".mv.db");
					String dbFileName = FilenameUtils.getName(dbFile.getAbsolutePath());
					if (dbFileName.contains(";")) {
						dbFileName = dbFileName.substring(0, dbFileName.indexOf(";"));
						String parentFolder = dbFile.getParent();
						dbFile = new File(parentFolder + "/" + dbFileName + ".mv.db");
					}
					if (!dbFile.exists()) {
						try {
							dbFile.getParentFile().mkdirs();
							dbFile.createNewFile();
						} catch (IOException e) {
							classLogger.error("Failed to create the database file {}: {}",
									Utility.cleanLogString(dbFile.getAbsolutePath()), e.getMessage(), e);
						}
					}
				}

				String port = PortAllocator.getInstance().getNextAvailablePort() + "";
				// create a random user and password
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + baseConnUrl;
				server.start();
			} catch (SQLException e) {
				classLogger.error("Failed to start the H2 TCP server for {}: {}", getEngineId(), e.getMessage(), e);
			}
		}

		if (serverUrl != null) {
			classLogger.info("{}{}{}", getEngineId(), DATABASE_RUNNING_ON, Utility.cleanLogString(serverUrl));
		}

		return serverUrl;
	}

	@Override
	public void close() throws IOException {
		stopServer();
		super.close();
	}

	/**
	 * Stops the TCP server this engine started, if it is running.
	 *
	 * <p>
	 * The server runs in this JVM and we hold the instance, so it is stopped
	 * directly. {@code Server.shutdownTcpServer} is the remote path: it signs in to
	 * the server's in-memory management database, and H2 generates a random
	 * management password for a server started without {@code -tcpPassword}, so
	 * that sign-in can never succeed from here. {@code Server.shutdown()} is no
	 * good either -- with no shutdown handler attached it stops every H2 server in
	 * the JVM rather than this one.
	 */
	private void stopServer() {
		if (server == null) {
			return;
		}
		try {
			server.stop();
		} catch (RuntimeException e) {
			classLogger.error("Failed to stop the H2 TCP server for {}: {}", getEngineId(), e.getMessage(), e);
		}
		server = null;
	}

	@Override
	public String getServerUrl() {
		return serverUrl;
	}
}
