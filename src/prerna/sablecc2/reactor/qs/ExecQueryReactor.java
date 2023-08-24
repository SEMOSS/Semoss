package prerna.sablecc2.reactor.qs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;

public class ExecQueryReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ExecQueryReactor.class);

	private NounMetadata qStruct = null;

	public ExecQueryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey(), "commit", ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		if (qStruct == null) {
			qStruct = getQueryStruct();
		}

		GenRowStruct commitGrs = this.store.getNoun("commit");
		Boolean commit = false;
		if(commitGrs != null && !commitGrs.isEmpty()) {
			commit = Boolean.parseBoolean(commitGrs.get(0) + "");
		}
		
		IDatabaseEngine engine = null;
		ITableDataFrame frame = null;
		AbstractQueryStruct qs = null;
		String userId = "user not defined";

		if (qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if (qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE || qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
				engine = qs.retrieveQueryStructEngine();
				if (!(engine instanceof BigDataEngine || engine instanceof IRDBMSEngine)) {
					throw new IllegalArgumentException("Query update/deletes only works for rdbms/rdf databases");
				}

				// If an engine and the user is defined, then grab it for the audit log
				User user = this.insight.getUser();
				if (user != null) {
					userId = user.getAccessToken(user.getLogins().get(0)).getId();
				}

				// check that the user can edit the engine
				if (!SecurityEngineUtils.userCanEditEngine(user, engine.getEngineId())) {
					throw new IllegalArgumentException("User does not have permission to exec query for this app");
				}
			} else if (qs.getQsType() == QUERY_STRUCT_TYPE.FRAME) {
				frame = qs.getFrame();
				if (!(frame instanceof AbstractRdbmsFrame)) {
					throw new IllegalArgumentException("Query update/deletes only works for sql frames");
				}
				// convert any aliases the FE is using from the frame
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, frame.getMetaData());
			}
		} else {
			throw new IllegalArgumentException("Input to exec query requires a query struct");
		}

		boolean update = false;
		boolean custom = false;
		String query = null;
		// grab query && determine how to store in audit db
		if (qs instanceof HardSelectQueryStruct) {
			query = ((HardSelectQueryStruct) qs).getQuery();
			custom = true;
		} else if (qs instanceof UpdateQueryStruct) {
			UpdateSqlInterpreter interp = new UpdateSqlInterpreter((UpdateQueryStruct) qs);
			interp.setUserId(userId);
			query = interp.composeQuery();
			update = true;
		} else if (qs instanceof SelectQueryStruct) {
			DeleteSqlInterpreter interp = new DeleteSqlInterpreter((SelectQueryStruct) qs);
			query = interp.composeQuery();
			update = false;
		}

		logger.info("EXEC QUERY.... " + query);
		if (qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE || qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			if(engine == null) {
				throw new NullPointerException("No engine passed in to execute the query");
			}
			try {
				engine.insertData(query);
				if(commit) {
					engine.commit();
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				String errorMessage = "An error occurred trying to execute the query in the database";
				if(e.getMessage() != null && !e.getMessage().isEmpty()) {
					errorMessage += ": " + e.getMessage();
				}
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(errorMessage));
			}
			// store query in audit db
			AuditDatabase audit = engine.generateAudit();
			if (custom) {
				audit.storeQuery(userId, query);
			} else {
				if (update) {
					audit.auditUpdateQuery((UpdateQueryStruct) qs, userId, query);
				} else {
					audit.auditDeleteQuery((SelectQueryStruct) qs, userId, query);
				}
			}

			ClusterUtil.pushDatabase(engine.getEngineId());
		} else {
			try {
				if (frame != null) {
					((AbstractRdbmsFrame) frame).getBuilder().runQuery(query);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				String errorMessage = "An error occurred trying to update the frame";
				if(e.getMessage() != null && !e.getMessage().isEmpty()) {
					errorMessage += ": " + e.getMessage();
				}
				throw new SemossPixelException(NounMetadata.getErrorNounMessage(errorMessage));
			}
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE, PixelOperationType.FORCE_SAVE_DATA_TRANSFORMATION);
		String customSuccessMessage = getCustomSuccessMessage();
		if(customSuccessMessage != null && !customSuccessMessage.isEmpty()) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(customSuccessMessage));
		}
		return noun;
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
	
	public String getCustomSuccessMessage() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.CUSTOM_SUCCESS_MESSAGE.getKey());
		if(grs != null && !grs.isEmpty()) {
			return (String) grs.get(0);
		}
		
		return null;
	}

	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}
}
