package prerna.ds.rdbms.postgres;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactor;

public class PostgresFrame extends AbstractRdbmsFrame {

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
		// build the connection url
		
		// I AM JUST HARD CODING THIS
		// CAUSE I AM LAZY AT THE MOMENT
		String host = "localhost";
		String port = "5432";
		this.schema = "testing";
		String username = "postgres";
		String password = "password";
		
		String connectionUrl = RdbmsConnectionHelper.getConnectionUrl(RdbmsTypeEnum.POSTGRES.getLabel(), host, port, this.schema, null);
		// get the connection
		this.conn = RdbmsConnectionHelper.getConnection(connectionUrl, username, password, RdbmsTypeEnum.POSTGRES.getLabel());
		// set the builder
		this.builder = new RdbmsFrameBuilder(this.conn, this.schema, this.util);
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
//		try {
//			if(!this.conn.isClosed()) {
//				String dropFrameSyntax = this.util.dropTable(this.frameName);
//				Statement stmt = this.conn.createStatement();
//				try {
//					stmt.execute(dropFrameSyntax);
//				} catch(SQLException e) {
//					e.printStackTrace();
//				} finally {
//					if(stmt != null) {
//						try {
//							stmt.close();
//						} catch (SQLException e) {
//							e.printStackTrace();
//						}
//					}
//				}
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
		// close all the other stuff
		super.close();
	}
	
}
