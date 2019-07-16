package prerna.ds.rdbms.sqlite;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import prerna.cache.CachePropFileFrameObject;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactor;

public class SQLiteFrame extends AbstractRdbmsFrame {

	private String fileLocation;
	
	public SQLiteFrame() {
		super();
	}

	public SQLiteFrame(String tableName) {
		super(tableName);
	}
	
	public SQLiteFrame(String[] headers) {
		super(headers);
	}
	
	public SQLiteFrame(String[] headers, String[] types) {
		super(headers, types);
	}
	
	protected void initConnAndBuilder() throws Exception {
		this.util = SqlQueryUtilFactor.initialize(RdbmsTypeEnum.SQLITE);

		// create the location of the file if it doesn't exist
		String folderToUsePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
				DIR_SEPARATOR + "SQLite_Store_" +  UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_");

		File folderToUse = new File(folderToUsePath);
		if(!folderToUse.exists()) {
			folderToUse.mkdirs();
		}
		
		this.fileLocation = folderToUsePath + DIR_SEPARATOR + "database.sqlite";
		// build the connection url
		String connectionUrl = RdbmsConnectionHelper.getConnectionUrl(RdbmsTypeEnum.SQLITE.getLabel(), fileLocation, null, null, null);
		// get the connection
		this.conn = RdbmsConnectionHelper.getConnection(connectionUrl, "", "", RdbmsTypeEnum.SQLITE.getLabel());
		// set the builder
		this.builder = new RdbmsFrameBuilder(this.conn, this.schema, this.util);
		this.util.enhanceConnection(this.conn);
	}
	
	@Override
	public void close() {
		super.close();
		File f = new File(this.fileLocation);
		if(f.exists()) {
			f.delete();
		}
	}
	
	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getName();
		cf.setFrameName(frameName);

		//save frame
		String frameFileName = folderDir + DIR_SEPARATOR + frameName + ".sqlite";
		
		String saveScript = "backup to '" + frameFileName + "'";
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.executeUpdate(saveScript);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException("Error occured attempting to cache SQL Frame", e);
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		if(!new File(frameFileName).exists()) {
			throw new IllegalArgumentException("Unable to save the SQLite frame");
		}
		if(new File(frameFileName).length() == 0){
			throw new IllegalArgumentException("Attempting to save an empty SQLite frame");
		}
		
		cf.setFrameCacheLocation(frameFileName);
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf) throws IOException {
		//set the frame name to that of the cached frame name
		this.frameName = cf.getFrameName();

		// load the frame
		String filePath = cf.getFrameCacheLocation();

		// drop the aggregate if it exists since the opening of the script will
		// fail otherwise
		Statement stmt = null;
		try {
			stmt = this.conn.createStatement();
			stmt.executeUpdate("restore from '" +  filePath + "'");
		} catch (SQLException e1) {
			e1.printStackTrace();
			throw new IOException("Error occured opening cached SQL Frame");
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		
		// open the meta details
		this.openCacheMeta(cf);
	}
}
