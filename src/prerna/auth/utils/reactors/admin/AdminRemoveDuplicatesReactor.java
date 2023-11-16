package prerna.auth.utils.reactors.admin;

import java.time.LocalDateTime;

import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AdminRemoveDuplicatesReactor extends AbstractReactor {

	private static final String CLASS_NAME = AdminRemoveDuplicatesReactor.class.getName();

	private static final String DROP_OLD_TABLE = "drop";

	public AdminRemoveDuplicatesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.TABLE.getKey(), DROP_OLD_TABLE};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}

		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String tableName = this.keyValue.get(this.keysToGet[1]);
		boolean dropTable = Boolean.parseBoolean( this.keyValue.get(this.keysToGet[2]) + "" );

		Logger logger = getLogger(CLASS_NAME);
		long start = System.currentTimeMillis();

		RDBMSNativeEngine app = (RDBMSNativeEngine) Utility.getDatabase(appId);
		AbstractSqlQueryUtil queryUtil = app.getQueryUtil();
		String oldTableName = "OLD_" + tableName + "_" + Utility.getLocalDateTimeUTC(LocalDateTime.now());
		oldTableName = oldTableName.replaceAll("[^A-Za-z0-9]", "_");

		String temp = "TEMP_" + Utility.getRandomString(6);
		temp = temp.replaceAll("[^A-Za-z0-9]", "_");

		// make a new table temp with all the same data
		String query1 = "CREATE TABLE " + temp + " AS SELECT DISTINCT * FROM " + tableName;
		String query2 = queryUtil.alterTableName(tableName, oldTableName);
		String query3 = queryUtil.alterTableName(temp, tableName);
		String query4 = queryUtil.dropTable(oldTableName);

		try {
			logger.info("Running query " + query1);
			app.insertData(query1);
			logger.info("Running query " + query2);
			app.insertData(query2);
			logger.info("Running query " + query3);
			app.insertData(query3);
			if(dropTable) {
				logger.info("Running query " + query4);
				app.insertData(query4);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}

		long end  = System.currentTimeMillis();
		return new NounMetadata("Time to complete the operation = " + (end-start) + "ms", PixelDataType.CONST_STRING);
	}

}
