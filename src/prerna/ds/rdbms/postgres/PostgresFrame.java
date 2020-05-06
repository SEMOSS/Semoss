package prerna.ds.rdbms.postgres;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactor;
import java.util.Properties;

import org.apache.log4j.Logger;

public class PostgresFrame extends AbstractRdbmsFrame {
	private static final String POSTGRES_HOST = "postgres_host";
	private static final String POSTGRES_PORT = "postgres_port";
	private static final String POSTGRES_USERNAME = "postgres_username";
	private static final String POSTGRES_PASSWORD = "postgres_password";
	private static final String TESTING = "testing";
	private static final String CONFIGURATION_FILE = "config.properties";
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public PostgresFrame() {
		super();
	}

	public PostgresFrame(String tableName) {
		super(tableName);
	}

	public PostgresFrame(String[] headers) {
		super(headers);
	}

	public PostgresFrame(String[] headers, String[] types) {
		super(headers, types);
	}

	@Override
	protected void initConnAndBuilder() throws Exception {
		this.util = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.POSTGRES);
		
		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
			Properties prop = new Properties();
			
			prop.load(input);
			
			String host = prop.getProperty(POSTGRES_HOST);
			String port = prop.getProperty(POSTGRES_PORT);
			this.schema = TESTING;
			String username = prop.getProperty(POSTGRES_USERNAME);
			String password = prop.getProperty(POSTGRES_PASSWORD);

			// build the connection url
			String connectionUrl = RdbmsConnectionHelper.getConnectionUrl(RdbmsTypeEnum.POSTGRES.getLabel(), host, port, this.schema, null);
			// get the connection
			this.conn = RdbmsConnectionHelper.getConnection(connectionUrl, username, password, RdbmsTypeEnum.POSTGRES.getLabel());
			// set the builder
			this.builder = new RdbmsFrameBuilder(this.conn, this.schema, this.util);
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file" + ex.getMessage());
		}
	}

	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		throw new IllegalArgumentException("tbd");
	}

	@Override
	public void open(CachePropFileFrameObject cf) throws IOException {
		throw new IllegalArgumentException("tbd");
	}
	
	@Override
	public void close() {
		// delete the table
		/*try {
			if (!this.conn.isClosed()) {
				String dropFrameSyntax = this.util.dropTable(this.frameName);
				Statement stmt = this.conn.createStatement();
				try {
					stmt.execute(dropFrameSyntax);
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					if (stmt != null) {
						try {
							stmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}*/
				// close all the other stuff
		super.close();
	}

	// Testing purposes
	public static void main(String[] args) {
		final Logger logger = Logger.getLogger(PostgresFrame.class.getName());
		DIHelper.getInstance().loadCoreProp("C:/workspace/Semoss_Dev/RDF_Map.prop");
		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
			Properties prop = new Properties();

			prop.load(input);

			logger.info("host: " + Utility.cleanLogString(prop.getProperty(POSTGRES_HOST)));
			logger.info("port: " + Utility.cleanLogString(prop.getProperty(POSTGRES_PORT)));
			//logger.info("username: " + Utility.cleanLogString(prop.getProperty(POSTGRES_USERNAME)));
			//logger.info("password: " + Utility.cleanLogString(prop.getProperty(POSTGRES_PASSWORD)));
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file" + ex.getMessage());
		}
	}
	
}
