package prerna.engine.impl.rdbms;

import java.sql.SQLException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.h2.tools.Server;

import prerna.util.Utility;

public class H2EmbeddedServerEngine extends RDBMSNativeEngine {

	private static final Logger LOGGER = LogManager.getLogger(H2EmbeddedServerEngine.class.getName());

	private Server server;
	private String serverUrl;
	
	@Override
	protected void init(RdbmsConnectionBuilder builder, boolean force) {
		String connectionUrl = builder.getConnectionUrl();
		if(connectionUrl.startsWith("jdbc:h2:nio:")) {
			connectionUrl = connectionUrl.substring("jdbc:h2:nio:".length());
		}
		if(force) {
			if(server != null) {
				try {
					Server.shutdownTcpServer(this.server.getURL(), "", true, false);
					server.shutdown();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				server = null;
			}
		}
		if (server == null) {
			try {
				String port = Utility.findOpenPort();
				// create a random user and password
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				serverUrl = "jdbc:h2:" + server.getURL() + "/nio:" + connectionUrl;
				server.start();
				
				// update the builder
				builder.setConnectionUrl(serverUrl);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		LOGGER.info(getEngineId() + " DATABASE RUNNING ON " + serverUrl);
		LOGGER.info(getEngineId() + " DATABASE RUNNING ON " + serverUrl);
		LOGGER.info(getEngineId() + " DATABASE RUNNING ON " + serverUrl);
	}
	
	
	
	@Override
	public void closeDB() {
		super.closeDB();
		try {
			Server.shutdownTcpServer(this.server.getURL(), "", true, false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.server.shutdown();
	}
}
