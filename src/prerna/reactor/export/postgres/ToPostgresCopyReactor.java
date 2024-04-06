package prerna.reactor.export.postgres;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class ToPostgresCopyReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPostgresCopyReactor.class);

	private static final String TABLE_NAME = "table";
	private static final String FORMAT = "format";
	private static final String NULL = "null";
	
	public ToPostgresCopyReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), 
				TABLE_NAME, FORMAT, NULL, ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
 	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have edit access to the database");
		}
		String tableName = this.keyValue.get(this.keysToGet[1]);
		String formatStr = this.keyValue.get(this.keysToGet[2]);
		if(formatStr == null || (formatStr=formatStr.trim()).isEmpty()) {
			formatStr = "csv";
		}
		String nullStr = this.keyValue.get(this.keysToGet[3]);
		String delimiterStr = this.keyValue.get(this.keysToGet[4]);

		String filePath = Utility.normalizePath(UploadInputUtility.getFilePath(this.store, this.insight));
		IEngine engine = Utility.getEngine(engineId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("Engine must be an RDBMS postgres engine");
		}
		
		long rowsInserted = 0;
		IRDBMSEngine database = (IRDBMSEngine) engine;
		Connection conn = null;
		try {
			String options = "FORMAT " + formatStr;
			if(nullStr != null) {
				options +=", null \"" + nullStr +"\"";
			}
			if(delimiterStr != null && !(delimiterStr=delimiterStr.trim()).isEmpty() ) {
				options +=", DELIMITER '" + delimiterStr + "'";
			}
			
			conn = database.getConnection();
			    rowsInserted = new CopyManager((BaseConnection) conn)
			            .copyIn("COPY " + tableName + " FROM STDIN (" + options + ", HEADER)", 
			                new BufferedReader(new FileReader(filePath))
			                );
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
		if(key.equals(TABLE_NAME)) {
			return "The name of the table we are copying into";
		} else if(key.equals(FORMAT)) {
			return "The format of the file being passed in. Default is csv";
		} else if(key.equals(NULL)) {
			return "What value is used to represent null";
		}
		return super.getDescriptionForKey(key);
	}
	
}
