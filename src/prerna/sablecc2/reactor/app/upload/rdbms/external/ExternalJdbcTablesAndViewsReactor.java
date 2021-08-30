package prerna.sablecc2.reactor.app.upload.rdbms.external;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class ExternalJdbcTablesAndViewsReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = ExternalJdbcTablesAndViewsReactor.class.getName();
	
	public ExternalJdbcTablesAndViewsReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.CONNECTION_DETAILS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		
		Map<String, Object> connectionDetails = getConDetails();
		if(connectionDetails != null) {
			String host = (String) connectionDetails.get(AbstractSqlQueryUtil.HOSTNAME);
			if(host != null) {
				String testUpdatedHost = this.insight.getAbsoluteInsightFolderPath(host);
				if(new File(testUpdatedHost).exists()) {
					host = testUpdatedHost;
					connectionDetails.put(AbstractSqlQueryUtil.HOSTNAME, host);
				}
			}
		}
		
		String driver = (String) connectionDetails.get(AbstractSqlQueryUtil.DRIVER_NAME);
		RdbmsTypeEnum driverEnum = RdbmsTypeEnum.getEnumFromString(driver);
		AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(driverEnum);
		
		Connection con = null;
		String connectionUrl = null;
		try {
			connectionUrl = queryUtil.buildConnectionString(connectionDetails);
		} catch (RuntimeException e) {
			throw new SemossPixelException(new NounMetadata("Unable to generation connection url with message " + e.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		try {
			con = AbstractSqlQueryUtil.makeConnection(queryUtil, connectionUrl, connectionDetails);
		} catch (SQLException e) {
			e.printStackTrace();
			String driverError = e.getMessage();
			String errorMessage = "Unable to establish connection given the connection details.\nDriver produced error: \" ";
			errorMessage += driverError;
			errorMessage += " \"";
			throw new SemossPixelException(new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		DatabaseMetaData meta;
		boolean close = false;
		try {
			meta = con.getMetaData();
		} catch (SQLException e) {
			close = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(new NounMetadata("Unable to get the database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		} finally {
			if(close) {
				queryUtil.closeAutoClosable(con, logger);
			}
		}
		
		String catalogFilter = null;
		try {
			catalogFilter = con.getCatalog();
		} catch (SQLException e) {
			// we can ignore this
			logger.error(Constants.STACKTRACE, e);
		}
		
		String schemaFilter = (String) connectionDetails.get(AbstractSqlQueryUtil.SCHEMA);
		if(schemaFilter == null) {
			schemaFilter = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, driverEnum);
		}
		Statement tableStmt = null;
		ResultSet tablesRs = null;
		try {
			tableStmt = con.createStatement();
			tablesRs = RdbmsConnectionHelper.getTables(con, tableStmt, meta, catalogFilter, schemaFilter, driverEnum);
		} catch (SQLException e) {
			close = true;
			throw new SemossPixelException(new NounMetadata("Unable to get tables and views from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		} finally {
			if(close) {
				queryUtil.closeAutoClosable(tablesRs, logger);
				queryUtil.closeAutoClosable(tableStmt, logger);
				queryUtil.closeAutoClosable(con, logger);
			}
		}
		
		Map<String, List<String>> ret = queryUtil.getTablesAndViews(con, tableStmt, tablesRs, connectionDetails, logger);
		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private Map<String, Object> getConDetails() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CONNECTION_DETAILS.getKey());
		if(grs != null && !grs.isEmpty()) {
			List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
			if(mapInput != null && !mapInput.isEmpty()) {
				return (Map<String, Object>) mapInput.get(0);
			}
		}
		
		List<Object> mapInput = grs.getValuesOfType(PixelDataType.MAP);
		if(mapInput != null && !mapInput.isEmpty()) {
			return (Map<String, Object>) mapInput.get(0);
		}
		
		return null;
	}

}
