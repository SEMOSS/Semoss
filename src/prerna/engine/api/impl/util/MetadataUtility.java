package prerna.engine.api.impl.util;

import java.util.List;

import prerna.engine.api.IDatabaseEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.util.Utility;

public class MetadataUtility {

	private MetadataUtility() {
		
	}
	
	public static boolean ignoreConceptData(IDatabaseEngine.DATABASE_TYPE type) {
		return type == IDatabaseEngine.DATABASE_TYPE.RDBMS
				|| type == IDatabaseEngine.DATABASE_TYPE.R
				|| type == IDatabaseEngine.DATABASE_TYPE.IMPALA
//				|| type == IDatabase.ENGINE_TYPE.JMES_API
				;
	}
	
	public static boolean ignoreConceptData(String engineId) {
		String eType = MasterDatabaseUtility.getDatabaseTypeForId(engineId);
		if(eType.startsWith("TYPE:")) {
			eType = eType.replace("TYPE:", "");
		}
		if(eType.equals("RDF")) {
			eType = "SESAME";
		}
		return ignoreConceptData(IDatabaseEngine.DATABASE_TYPE.valueOf(eType));
	}
	
	/**
	 * Check if a given property name is already present in an existing concept
	 * @param engine
	 * @param conceptPhysicalUri
	 * @param propertyName
	 * @return
	 */
	public static boolean propertyExistsForConcept(IDatabaseEngine engine, String conceptPhysicalUri, String propertyName) {
		List<String> properties = engine.getPropertyUris4PhysicalUri(conceptPhysicalUri);
		for(String prop : properties) {
			if(propertyName.equalsIgnoreCase(Utility.getInstanceName(prop))) {
				return true;
			}
		}
		
		return false;
	}
	
}
