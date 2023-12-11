package prerna.ds.rdbms.postgres;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.crypto.Cipher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.PostgresSqlInterpreter;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class PostgresFrame extends AbstractRdbmsFrame {
	
	private Logger logger = LogManager.getLogger(PostgresFrame.class);
	
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
		this.util = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.POSTGRES);
		
		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
			Properties prop = new Properties();
			
			prop.load(input);
			
			String host = prop.getProperty(POSTGRES_HOST);
			String port = prop.getProperty(POSTGRES_PORT);
			this.schema = TESTING;
			String username = prop.getProperty(POSTGRES_USERNAME);
			String password = prop.getProperty(POSTGRES_PASSWORD);

			// build the connection url
			Map<String, Object> connDetails = new HashMap<>();
			connDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
			connDetails.put(AbstractSqlQueryUtil.PORT, port);
			connDetails.put(AbstractSqlQueryUtil.SCHEMA, this.schema);
			String connectionUrl = this.util.setConnectionDetailsfromMap(connDetails);
			// get the connection
			this.conn = AbstractSqlQueryUtil.makeConnection(RdbmsTypeEnum.POSTGRES, connectionUrl,  username, password);
			
			// set the builder
			this.builder = new RdbmsFrameBuilder(this.conn, this.database, this.schema, this.util);
		} catch (IOException ex) {
			logger.error("Error with loading properties in config file" + ex.getMessage());
		}
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		throw new IllegalArgumentException("tbd");
	}

	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
		throw new IllegalArgumentException("tbd");
	}
	
	@Override
	public IQueryInterpreter getQueryInterpreter() {
		return new PostgresSqlInterpreter(this);
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
//
//	// Testing purposes
//	public static void main(String[] args) {
//		final Logger logger = LogManager.getLogger(PostgresFrame.class.getName());
//		DIHelper.getInstance().loadCoreProp("C:/workspace/Semoss_Dev/RDF_Map.prop");
//		try(InputStream input = new FileInputStream(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + CONFIGURATION_FILE)) {
//			Properties prop = new Properties();
//
//			prop.load(input);
//
//			logger.info("host: " + Utility.cleanLogString(prop.getProperty(POSTGRES_HOST)));
//			logger.info("port: " + Utility.cleanLogString(prop.getProperty(POSTGRES_PORT)));
//			//logger.info("username: " + Utility.cleanLogString(prop.getProperty(POSTGRES_USERNAME)));
//			//logger.info("password: " + Utility.cleanLogString(prop.getProperty(POSTGRES_PASSWORD)));
//		} catch (IOException ex) {
//			logger.error("Error with loading properties in config file" + ex.getMessage());
//		}
//	}
	
}
