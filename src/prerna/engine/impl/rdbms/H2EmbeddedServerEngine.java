package prerna.engine.impl.rdbms;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.Server;

import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Utility;

public class H2EmbeddedServerEngine extends RDBMSNativeEngine {

	private static final Logger logger = LogManager.getLogger(H2EmbeddedServerEngine.class);
	private static final String DATABASE_RUNNING_ON = " DATABASE RUNNING ON ";

	private Server server;
	private String serverUrl;
	
	@Override
	protected String init(String connectionUrl, boolean force) {
		String baseConnUrl = connectionUrl;
		if(baseConnUrl.startsWith("jdbc:h2:nio:")) {
			baseConnUrl = baseConnUrl.substring("jdbc:h2:nio:".length());
		}
		if(force && server != null) {
			try {
				Server.shutdownTcpServer(this.server.getURL(), "", true, false);
				server.shutdown();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			server = null;
		}
		if (server == null) {
			try {
				// make sure the database file exists if it does not
				{
					File dbFile = new File(baseConnUrl + ".mv.db");
					String dbFileName = FilenameUtils.getName(dbFile.getAbsolutePath());
					if(dbFileName.contains(";")) {
						dbFileName = dbFileName.substring(0, dbFileName.indexOf(";"));
						String parentFolder = dbFile.getParent();
						dbFile = new File(parentFolder + "/" + dbFileName + ".mv.db");
					}
					if(!dbFile.exists()) {
						try {
							dbFile.getParentFile().mkdirs();
							dbFile.createNewFile();
						} catch (IOException e) {
							logger.error(Constants.STACKTRACE, e);
						}
					}
				}
				
				String port = PortAllocator.getInstance().getNextAvailablePort()+"";
				// create a random user and password
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + baseConnUrl;
				server.start();
			} catch (SQLException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}

		logger.info(getEngineId() + DATABASE_RUNNING_ON + Utility.cleanLogString(serverUrl));
		logger.info(getEngineId() + DATABASE_RUNNING_ON + Utility.cleanLogString(serverUrl));
		logger.info(getEngineId() + DATABASE_RUNNING_ON + Utility.cleanLogString(serverUrl));
		
		return serverUrl;
	}

	@Override
	public void close() throws IOException {
		try {
			Server.shutdownTcpServer(this.server.getURL(), "", true, false);
			this.server.shutdown();
		} catch (SQLException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		super.close();
	}

	public String getServerUrl() {
		return serverUrl;
	}
}
