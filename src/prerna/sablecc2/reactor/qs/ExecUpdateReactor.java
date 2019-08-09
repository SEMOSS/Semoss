package prerna.sablecc2.reactor.qs;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ExecUpdateReactor extends AbstractReactor {

	private NounMetadata qStruct = null;

	public ExecUpdateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		if (qStruct == null) {
			qStruct = getQueryStruct();
		}

		IEngine engine = null;
		AbstractQueryStruct qs = null;
		String userId = "user not defined";
		String query = null;

		if (qStruct.getValue() instanceof AbstractQueryStruct) {
			qs = ((AbstractQueryStruct) qStruct.getValue());
			if (qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
				engine = qs.retrieveQueryStructEngine();
				query = ((HardSelectQueryStruct) qs).getQuery();
				if (!(engine instanceof RDBMSNativeEngine)) {
					throw new IllegalArgumentException("ExecUpdate only works for rdbms databases");
				}

				// If an engine and the user is defined, then grab it for the
				// audit log
				User user = this.insight.getUser();
				if (user != null) {
					userId = user.getAccessToken(user.getLogins().get(0)).getId();
				}

				// If security is enabled, then check that the user can edit the
				// engine
				if (AbstractSecurityUtils.securityEnabled()) {
					if (!SecurityAppUtils.userCanEditEngine(user, engine.getEngineId())) {
						throw new IllegalArgumentException("User does not have permission to ExecUpdate for this app");
					}
				}
			}
		} else {
			throw new IllegalArgumentException("Input to ExecUpdate requires a query struct");
		}

		System.out.println("SQL QUERY...." + query);
		if (engine != null && query != null) {
			try {
				engine.insertData(query);
				AuditDatabase audit = ((RDBMSNativeEngine) engine).generateAudit();
				audit.storeQuery(userId, query);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(NounMetadata
						.getErrorNounMessage("An error occured trying to execute the query in the database"));
			}

		}

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.ALTER_DATABASE);
	}

	private NounMetadata getQueryStruct() {
		NounMetadata object = new NounMetadata(null, PixelDataType.QUERY_STRUCT);
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		NounMetadata f = new NounMetadata(false, PixelDataType.BOOLEAN);
		if (allNouns != null) {
			object = (NounMetadata) allNouns.getNoun(0);
			return object;
		}
		return f;
	}

	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}
}