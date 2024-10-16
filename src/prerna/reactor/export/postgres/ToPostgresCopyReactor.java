package prerna.reactor.export.postgres;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.Utility;

public class ToPostgresCopyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPostgresCopyReactor.class);

	private static final String FORMAT = "format";
	private static final String NULL = "nullValue";
	
	public ToPostgresCopyReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), 
				ReactorKeysEnum.TABLE.getKey(), FORMAT, NULL, ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
 	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		organizeKeys();
		
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have edit access to the database");
		}
		String tableName = this.keyValue.get(this.keysToGet[1]);
		String formatStr = this.keyValue.get(this.keysToGet[2]);
		if(formatStr == null || (formatStr=formatStr.trim()).isEmpty()) {
			formatStr = "csv";
		}
		String nullStr = this.keyValue.get(this.keysToGet[3]);
		String delimiterStr = this.keyValue.get(this.keysToGet[4]);

		String filePath = Utility.normalizePath(UploadInputUtility.getExtendedFilePath(this.store, this.insight));
		IEngine engine = Utility.getEngine(engineId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Engine must be an RDBMS postgres engine");
		}
		
		long rowsInserted = 0;
		IRDBMSEngine database = (IRDBMSEngine) engine;
		Connection conn = null;
		String options = "FORMAT " + formatStr;
		try {
			if(nullStr != null) {
				options +=", null \"" + nullStr +"\"";
			}
			if(delimiterStr != null && !(delimiterStr=delimiterStr.trim()).isEmpty() ) {
				options +=", DELIMITER '" + delimiterStr + "'";
			}
			
			conn = database.getConnection();
			Connection postgresConn = null;
			if(database.isConnectionPooling()) {
				postgresConn = conn.unwrap(Connection.class);
			} else {
				postgresConn = conn;
			}
			int majorV = conn.getMetaData().getDatabaseMajorVersion();
			// due to changes in postgres version
			// we will do a header match if version 15+
			String copySyntax = null;
			if(majorV >= 15) {
				copySyntax = "COPY " + tableName + " FROM STDIN (" + options + ", HEADER MATCH)";
			} else {
				copySyntax = "COPY " + tableName + " FROM STDIN (" + options + ", HEADER)";
			}
			
			classLogger.info(User.getSingleLogginName(user) + " is running copy command on " + engineId + " with sql: " + copySyntax 
					+ " on file " + filePath);
			rowsInserted = new CopyManager((BaseConnection) postgresConn)
	            .copyIn(copySyntax, 
	                new BufferedReader(new FileReader(filePath))
	                );
			classLogger.info(User.getSingleLogginName(user) + " is ran copy command on " + engineId + " and added " + rowsInserted + " rows");
			
			// commit the copy
			if(!postgresConn.getAutoCommit()) {
				postgresConn.commit();
			}
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing the COPY command. Detailed message = " + e.getMessage());
		} finally {
			if(database.isConnectionPooling()) {
				ConnectionUtils.closeConnection(conn);
			}
		}
		
		return new NounMetadata(rowsInserted, PixelDataType.CONST_INT);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.TABLE.getKey())) {
			return "The name of the table we are copying into";
		} else if(key.equals(FORMAT)) {
			return "The format of the file being passed in. Default is csv";
		} else if(key.equals(NULL)) {
			return "What value is used to represent null";
		}
		return super.getDescriptionForKey(key);
	}
	
}
