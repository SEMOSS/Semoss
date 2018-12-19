package prerna.sablecc2.reactor.qs.source;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Utility;

public class AuditDatabaseReactor extends AbstractQueryStructReactor {
	
	public AuditDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("Database " + appId + " does not exist or user does not have access to database");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("Database " + appId + " does not exist");
			}
		}
	
		if (!(Utility.getEngine(appId) instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}
		
		// get audit database from app id
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(appId);
		AuditDatabase audit = engine.generateAudit();
		
		RDBMSNativeEngine fakeEngine = new RDBMSNativeEngine();
		fakeEngine.setEngineId("FAKE_ENGINE");
		fakeEngine.setConnection(audit.getConnection());
		fakeEngine.setBasic(true);
		
		this.qs.setEngine(fakeEngine);
		this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
		return this.qs;
	}
}