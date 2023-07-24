package prerna.sablecc2.reactor.tax;

import java.util.Map;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class SaveTaxScenarioReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		
		
		return null;
	}
	
	public static boolean addNewVersion(IDatabase engine, String clientID, double scenarioID, double latestVersion, double newVersion, Map<String, String> newValues) {
		clientID = RdbmsQueryBuilder.escapeForSQLStatement(clientID);
		String sql = "Insert Into INPUTCSV (CLIENT_ID, SCENARIO, VERSION, FORMNAME, FIELDNAME, ALIAS_1, VALUE_1, TYPE_1, HASHCODE, WEIGHT, RETURNTYPE, COLUMN_1) "
				+ "Select CLIENT_ID, SCENARIO, " + newVersion + ", FORMNAME, FIELDNAME, ALIAS_1, VALUE_1, TYPE_1, HASHCODE, WEIGHT, RETURNTYPE, COLUMN_1 From INPUTCSV "
				+ "WHERE CLIENT_ID='" + clientID + "' AND SCENARIO=" + scenarioID + " AND VERSION=" + latestVersion;
		
		// execute inserts
		((RDBMSNativeEngine) engine).execUpdateAndRetrieveStatement(sql, true);
		
		for(String alias : newValues.keySet()) {
			alias = RdbmsQueryBuilder.escapeForSQLStatement(alias);
			String aliasValue = RdbmsQueryBuilder.escapeForSQLStatement(newValues.get(alias));
			sql = "UPDATE INPUTCSV SET VALUE_1='" + aliasValue + "' "
					+ "WHERE CLIENT_ID='" + clientID + "' AND SCENARIO=" + scenarioID + " AND VERSION=" + newVersion + " AND ALIAS_1='" + alias + "'";
			
			//execute individual update
			((RDBMSNativeEngine) engine).execUpdateAndRetrieveStatement(sql, true);
		}
		
		return true;
	}
}
