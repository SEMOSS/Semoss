package prerna.ds.rdbms.h2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import javax.crypto.Cipher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

import prerna.cache.CachePropFileFrameObject;
import prerna.ds.QueryStruct;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.rdbms.RdbmsFrameBuilder;
import prerna.om.ThreadStore;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.RelationSet;
import prerna.reactor.imports.RdbmsImporter;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class H2Frame extends AbstractRdbmsFrame {

	private Logger logger = LogManager.getLogger(H2Frame.class);
	
	private String fileLocation;
	private String fileNameToUse;

	private Server server = null;
	private String serverURL = null;
	private Map<String, String[]> tablePermissions = null;

	public H2Frame() {
		super();
	}

	public H2Frame(String tableName) {
		super(tableName);
	}

	public H2Frame(String[] headers) {
		super(headers);
	}

	public H2Frame(String[] headers, String[] types) {
		super(headers, types);
	}

	protected void initConnAndBuilder() throws Exception {
		this.util = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);

		String sessionId = ThreadStore.getSessionId();
		String insightId = ThreadStore.getInsightId();

		String folderToUsePath = null;
		if(sessionId != null && insightId != null) {
			sessionId = InsightUtility.getFolderDirSessionId(sessionId);
			folderToUsePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
					DIR_SEPARATOR + sessionId +  DIR_SEPARATOR + insightId;
			this.fileNameToUse = "H2_Store_" +  UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_") + ".mv.db";
		} else {
			folderToUsePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + 
					DIR_SEPARATOR + "H2_Store_" +  UUID.randomUUID().toString().toUpperCase().replaceAll("-", "_");
			this.fileNameToUse = "database.mv.db";
		}

		// create the location of the file if it doesn't exist
		File folderToUse = new File(folderToUsePath);
		if(!folderToUse.exists()) {
			folderToUse.mkdirs();
		}

		this.fileLocation = folderToUsePath + DIR_SEPARATOR + this.fileNameToUse;
		// make the actual file so the connection helper knows its not a tcp protocol
		File fileToUse = new File(this.fileLocation);
		if(!fileToUse.exists()) {
			fileToUse.createNewFile();
		}

		// build the connection url
		Map<String, Object> connDetails = new HashMap<>();
		connDetails.put(AbstractSqlQueryUtil.HOSTNAME, fileLocation);
		connDetails.put(AbstractSqlQueryUtil.ADDITIONAL, "LOG=0;CACHE_SIZE=65536;LOCK_MODE=1;UNDO_LOG=0");
		String connectionUrl = this.util.setConnectionDetailsfromMap(connDetails);
		// get the connection
		this.conn = AbstractSqlQueryUtil.makeConnection(RdbmsTypeEnum.H2_DB, connectionUrl,  "sa", "");
		// set the builder
		this.builder = new RdbmsFrameBuilder(this.conn, this.database, this.schema, this.util);
		this.util.enhanceConnection(this.conn);
	}

	@Override
	public void close() {
		super.close();
		File f = new File(Utility.normalizePath(this.fileLocation));
		DeleteDbFiles.execute(f.getParent().replace('\\','/'), this.fileNameToUse.replace(".mv.db", ""), false);
		if(f.exists()) {
			f.delete();
		}
		// also delete the parent folder
		File pF = f.getParentFile();
		// this condition should always be the case
		if(pF.listFiles() != null && pF.listFiles().length == 0) {
			f.getParentFile().delete();
		}
	}

	@Override
	public CachePropFileFrameObject save(String folderDir, Cipher cipher) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();

		String frameName = this.getName();
		cf.setFrameName(frameName);

		//save frame
		String frameFileName = folderDir + DIR_SEPARATOR + frameName + ".gz";

		String saveScript = "SCRIPT TO '" + frameFileName + "' COMPRESSION GZIP TABLE " + frameName;
		Statement stmt = null;
		try {
			// removing our custom aggregates so the file could be used/loaded elsewhere
			stmt = this.conn.createStatement();
			stmt.execute("DROP AGGREGATE IF EXISTS SMSS_MEDIAN");
			stmt.close();
			stmt = this.conn.createStatement();
			stmt.execute(saveScript);
		} catch (Exception e) {
			throw new IOException("Error occurred attempting to cache SQL Frame", e);
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
			throw new IllegalArgumentException("Unable to save the H2 frame");
		}
		if(new File(frameFileName).length() == 0){
			throw new IllegalArgumentException("Attempting to save an empty H2 frame");
		}

		cf.setFrameCacheLocation(frameFileName);
		// also save the meta details
		this.saveMeta(cf, folderDir, frameName, cipher);
		return cf;
	}

	@Override
	public void open(CachePropFileFrameObject cf, Cipher cipher) throws IOException {
		//set the frame name to that of the cached frame name
		this.frameName = cf.getFrameName();

		// load the frame
		String filePath = Utility.normalizePath(cf.getFrameCacheLocation());

		// drop the aggregate if it exists since the opening of the script will
		// fail otherwise
		//		Statement stmt = null;
		//		try {
		//			stmt = this.conn.createStatement();
		//			stmt.executeUpdate("DROP AGGREGATE IF EXISTS MEDIAN");
		//		} catch (SQLException e1) {
		//			e1.printStackTrace();
		//		} finally {
		//			if(stmt != null) {
		//				try {
		//					stmt.close();
		//				} catch (SQLException e) {
		//					e.printStackTrace();
		//				}
		//			}
		//		}

		Reader r = null;
		GZIPInputStream gis = null;
		FileInputStream fis = null;
		try {
			//load the frame
			fis = new FileInputStream(filePath);
			gis = new GZIPInputStream(fis);
			r = new InputStreamReader(gis);
			RunScript.execute(this.conn, r);
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException("Error occurred opening cached SQL Frame");
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
				if(gis != null) {
					gis.close();
				}
				if(r != null) {
					r.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}


			// open the meta details
			this.openCacheMeta(cf, cipher);
		}
	}

	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////

	/*
	 * Random methods I am pulling from the old H2Builder
	 * Not specifically used in any workflow at the moment
	 */

	public String connectFrame(String pass) {
		if (server == null) {
			try {
				String port = Utility.findOpenPort();
				// create a random user and password
				// get the connection object and start up the frame
				server = Server.createTcpServer("-tcpPort", port, "-tcpAllowOthers");
				// server = Server.createPgServer("-baseDir", "~",
				// "-pgAllowOthers"); //("-tcpPort", "9999");
				serverURL = "jdbc:h2:" + server.getURL() + "/nio:" + this.schema;
				server.start();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		printSchemaTables(pass);
		System.out.println("URL... " + serverURL);
		return serverURL;
	}

	private void printSchemaTables(String pass) {
		Connection conn = null;
		ResultSet rs = null;
		try {
			Class.forName("org.h2.Driver");
			String url = serverURL;
			conn = DriverManager.getConnection(url, "sa", pass);
			rs = conn.createStatement()
					.executeQuery("SELECT TABLE_NAME FROM INFORMATIOn_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'");

			while (rs.next()) {
				System.out.println("Table name is " + rs.getString(1));
			}
			
			// String schema = this.conn.getSchema();
			System.out.println(".. " + conn.getMetaData().getURL());
			System.out.println(".. " + conn.getMetaData().getUserName());
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} finally {
			if(conn!=null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String[] createUser(String tableName) {
		if(tablePermissions == null) {
			tablePermissions = new Hashtable<>();
		}
		// really simple
		// find an open port
		// once found
		// create url with connection string and send it back

		// need to pass the username and password back
		// the username is specific to an insight and possibly gives access only
		// to that insight
		// I need to get the insight table - i.e. the table backing the insight
		String[] retString = new String[2];
		Statement stmt = null;
		if (!tablePermissions.containsKey(tableName)) {
			try {

				// create a random user and password
				stmt = conn.createStatement();
				String userName = Utility.getRandomString(23);
				String password = Utility.getRandomString(23);
				retString[0] = userName;
				retString[1] = password;
				String query = "CREATE USER " + userName + " PASSWORD '" + password + "'";

				stmt.executeUpdate(query);

				// should not give admin permission
				// query = "ALTER USER " + userName + " ADMIN TRUE";

				// create a new role for this table
				query = "CREATE ROLE IF NOT EXISTS " + tableName + "READONLY";
				stmt.executeUpdate(query);
				query = "GRANT SELECT, INSERT, UPDATE ON " + tableName + " TO " + tableName + "READONLY";
				stmt.executeUpdate(query);

				// assign this to our new user
				query = "GRANT " + tableName + "READONLY TO " + userName;
				stmt.executeUpdate(query);

				//System.out.println("username " + userName);
				//System.out.println("Pass word " + password);

				tablePermissions.put(tableName, retString);
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				if(stmt != null){
					try {
						stmt.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		return tablePermissions.get(tableName);
	}

	public void disconnectFrame() {
		server.stop();
		server = null;
		serverURL = null;
	}

	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////

	/*
	 * Legacy calls
	 */

	/**
	 * Execute and get back a result set
	 * Responsibility of the method to grab the statement and close it 
	 * from the result set
	 * @param query
	 * @return
	 */
	public ResultSet execQuery(String query) {
		PreparedStatement stmt = null;
		boolean error = false;
		try {
			//Statement stmt = this.conn.createStatement();
			stmt = this.conn.prepareStatement(query);
			return stmt.executeQuery();
		} catch (SQLException e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
		} finally {
			// it is the responsibility of the code executing this 
			// to take the statement and close it if no error
			if(error && stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	public void deleteAllRows() {
		String query = "DELETE FROM " + this.frameName + " WHERE 1 != 0";
		try {
			this.builder.runQuery(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
		logger.info("Processing Component..................................");

		List<ISEMOSSTransformation> preTrans = component.getPreTrans();
		Vector<Map<String, String>> joinColList = new Vector<Map<String, String>>();
		String joinType = null;
		List<prerna.sablecc2.om.Join> joins = new ArrayList<prerna.sablecc2.om.Join>();
		for (ISEMOSSTransformation transformation : preTrans) {
			if (transformation instanceof JoinTransformation) {
				Map<String, String> joinMap = new HashMap<String, String>();
				String joinCol1 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_ONE_KEY);
				String joinCol2 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_TWO_KEY);
				joinType = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.JOIN_TYPE);
				joinMap.put(joinCol2, joinCol1); // physical in query struct
				// ----> logical in existing
				// data maker
				prerna.sablecc2.om.Join colJoin = new prerna.sablecc2.om.Join(this.getName()+"__"+joinCol1, joinType, joinCol2);
				joins.add(colJoin);
				joinColList.add(joinMap);
			}
		}

		// logic to flush out qs -> qs2
		QueryStruct qs = component.getQueryStruct();
		// the component will either have a qs or a query string, account for that here
		SelectQueryStruct qs2 = null;
		if (qs == null) {
			String query = component.getQuery();
			qs2 = new HardSelectQueryStruct();
			((HardSelectQueryStruct) qs2).setQuery(query);
			qs2.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			qs2 = new SelectQueryStruct();
			// add selectors
			Map<String, List<String>> qsSelectors = qs.getSelectors();
			for (String key : qsSelectors.keySet()) {
				for (String prop : qsSelectors.get(key)) {
					qs2.addSelector(key, prop);
				}
			}
			Set<IRelation> rels = new RelationSet();
			Map<String, Map<String, List>> curRels = qs.getRelations();
			for(String up : curRels.keySet()) {
				Map<String, List> innerMap = curRels.get(up);
				for(String jType : innerMap.keySet()) {
					List downs = innerMap.get(jType);
					for(Object d : downs) {
						rels.add(new BasicRelationship(new String[]{up, jType, d.toString()}));
					}
				}
			}
			qs2.mergeRelations(rels);
			qs2.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		}

		long time1 = System.currentTimeMillis();
		// set engine on qs2
		qs2.setEngineId(component.getEngineName());
		// instantiate h2importer with frame and qs
		RdbmsImporter importer = new RdbmsImporter(this, qs2);
		if (joins.isEmpty()) {
			importer.insertData();
		} else {
			importer.mergeData(joins);
		}

		long time2 = System.currentTimeMillis();
		logger.info(" Processed Merging Data: " + (time2 - time1) + " ms");
	}

	//	@Override
	//	@Deprecated
	//	public Map<String, String> getScriptReactors() {
	//		Map<String, String> reactorNames = super.getScriptReactors();
	//		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
	//		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
	//		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
	//		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
	//		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
	//		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
	//		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
	//		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
	//		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
	//		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
	//		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
	//		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
	//		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
	//		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
	//		
	//		// h2 specific reactors
	//		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.H2ColAddReactor");
	//		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.H2ColSplitReactor");
	//		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.H2ImportDataReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DuplicatesReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME_CHANGE_TYPE, "prerna.sablecc.H2ChangeTypeReactor");
	//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.H2VizReactor");
	////		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME_SET_EDGE_HASH, "prerna.sablecc.FlatTableSetEdgeHash");
	//
	//		// rdbms connection logic
	//		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
	//		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
	//		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
	//		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DuplicatesReactor");
	//		reactorNames.put(PKQLEnum.COL_FILTER_MODEL, "prerna.sablecc.H2ColFilterModelReactor");
	//		
	//		// h2 specific expression handlers		
	//		reactorNames.put(PKQLEnum.SUM, "prerna.sablecc.expressions.sql.SqlSumReactor");
	//		reactorNames.put(PKQLEnum.MAX, "prerna.sablecc.expressions.sql.SqlMaxReactor");
	//		reactorNames.put(PKQLEnum.MIN, "prerna.sablecc.expressions.sql.SqlMinReactor");
	//		reactorNames.put(PKQLEnum.AVERAGE, "prerna.sablecc.expressions.sql.SqlAverageReactor");
	//		reactorNames.put(PKQLEnum.COUNT, "prerna.sablecc.expressions.sql.SqlCountReactor");
	//		reactorNames.put(PKQLEnum.COUNT_DISTINCT, "prerna.sablecc.expressions.sql.SqlUniqueCountReactor");
	//		reactorNames.put(PKQLEnum.CONCAT, "prerna.sablecc.expressions.sql.SqlConcatReactor");
	//		reactorNames.put(PKQLEnum.GROUP_CONCAT, "prerna.sablecc.expressions.sql.SqlGroupConcatReactor");
	//		reactorNames.put(PKQLEnum.UNIQUE_GROUP_CONCAT, "prerna.sablecc.expressions.sql.SqlDistinctGroupConcatReactor");
	//		reactorNames.put(PKQLEnum.ABSOLUTE, "prerna.sablecc.expressions.sql.SqlAbsoluteReactor");
	//		reactorNames.put(PKQLEnum.ROUND, "prerna.sablecc.expressions.sql.SqlRoundReactor");
	//		reactorNames.put(PKQLEnum.COS, "prerna.sablecc.expressions.sql.SqlCosReactor");
	//		reactorNames.put(PKQLEnum.SIN, "prerna.sablecc.expressions.sql.SqlSinReactor");
	//		reactorNames.put(PKQLEnum.TAN, "prerna.sablecc.expressions.sql.SqlTanReactor");
	//		reactorNames.put(PKQLEnum.CEILING, "prerna.sablecc.expressions.sql.SqlCeilingReactor");
	//		reactorNames.put(PKQLEnum.FLOOR, "prerna.sablecc.expressions.sql.SqlFloorReactor");
	//		reactorNames.put(PKQLEnum.LOG, "prerna.sablecc.expressions.sql.SqlLogReactor");
	//		reactorNames.put(PKQLEnum.LOG10, "prerna.sablecc.expressions.sql.SqlLog10Reactor");
	//		reactorNames.put(PKQLEnum.SQRT, "prerna.sablecc.expressions.sql.SqlSqrtReactor");
	//		reactorNames.put(PKQLEnum.POWER, "prerna.sablecc.expressions.sql.SqlPowerReactor");
	//		reactorNames.put(PKQLEnum.CORRELATION_ALGORITHM, "prerna.ds.h2.H2CorrelationReactor");
	//
	//		// default to sample stdev
	//		reactorNames.put(PKQLEnum.STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlSampleStandardDeviationReactor");
	//		reactorNames.put(PKQLEnum.SAMPLE_STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlSampleStandardDeviationReactor");
	//		reactorNames.put(PKQLEnum.POPULATION_STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlPopulationStandardDeviationReactor");
	////		reactorNames.put(PKQLEnum.MEDIAN, "prerna.sablecc.expressions.sql.SqlMedianReactor");
	//		
	//		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
	//		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.CsvApiReactor");
	//		reactorNames.put(PKQLEnum.EXCEL_API, "prerna.sablecc.ExcelApiReactor");
	//		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
	//		reactorNames.put(PKQLEnum.FRAME_API, "prerna.sablecc.H2ApiReactor");
	//		reactorNames.put(PKQLEnum.FRAME_RAW_API, "prerna.sablecc.H2RawQueryApiReactor");
	//
	//		reactorNames.put(PKQLEnum.CLEAR_DATA, "prerna.sablecc.H2ClearDataReactor");
	//		
	//		return reactorNames;
	//	}
}

