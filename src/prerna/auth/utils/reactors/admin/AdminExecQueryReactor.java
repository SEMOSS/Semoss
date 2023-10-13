package prerna.auth.utils.reactors.admin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AdminExecQueryReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(AdminExecQueryReactor.class);

	private NounMetadata qStruct = null;

	public AdminExecQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (qStruct == null) {
			qStruct = getQueryStruct();
		}

		IDatabaseEngine engine = null;
		AbstractQueryStruct qs = null;
		
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		String userId = user.getAccessToken(user.getLogins().get(0)).getId();

		if (qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if (qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE || qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
				engine = qs.retrieveQueryStructEngine();
				if(engine == null) {
					throw new NullPointerException("No engine passed in to execute the query");
				}
				if (!(engine instanceof BigDataEngine || engine instanceof RDBMSNativeEngine)) {
					throw new IllegalArgumentException("Query update/deletes only works for rdbms/rdf databases");
				}
			} else {
				throw new IllegalArgumentException("Input to admin exec query requires a query struct on an engine");
			}
		} else {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}

		// used for audit but we dont use this for admin databases
		boolean update = false;
		boolean custom = false;
		
		String query = null;
		// grab query && determine how to store in audit db
		if (qs instanceof HardSelectQueryStruct) {
			query = ((HardSelectQueryStruct) qs).getQuery();
			custom = true;
		} else if (qs instanceof UpdateQueryStruct) {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter((UpdateQueryStruct) qs);
			query = interp.composeQuery();
			update = true;
		} else if (qs instanceof SelectQueryStruct) {
			DeleteSqlInterpreter interp = new DeleteSqlInterpreter((SelectQueryStruct) qs);
			query = interp.composeQuery();
			update = false;
		}

		logger.info("EXEC QUERY.... " + query);
		try {
			engine.insertData(query);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			String errorMessage = "An error occurred trying to execute the query in the database";
			if(e.getMessage() != null && !e.getMessage().isEmpty()) {
				errorMessage += ": " + e.getMessage();
			}
			throw new SemossPixelException(NounMetadata.getErrorNounMessage(errorMessage));
		}
		
//		// store query in audit db
//		try {
//			AuditDatabase audit = engine.generateAudit();
//			if(audit != null) {
//				if (custom) {
//					audit.storeQuery(userId, query);
//				} else {
//					if (update) {
//						audit.auditUpdateQuery((UpdateQueryStruct) qs, userId, query);
//					} else {
//						audit.auditDeleteQuery((SelectQueryStruct) qs, userId, query);
//					}
//				}
//			}
//		} catch(Exception e) {
//			logger.error(Constants.STACKTRACE, e);
//		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
	}

	private NounMetadata getQueryStruct() {
		NounMetadata object = new NounMetadata(null, PixelDataType.QUERY_STRUCT);
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey());
		NounMetadata f = new NounMetadata(false, PixelDataType.BOOLEAN);
		if (allNouns != null) {
			object = allNouns.getNoun(0);
			return object;
		}
		return f;
	}

	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}
}
