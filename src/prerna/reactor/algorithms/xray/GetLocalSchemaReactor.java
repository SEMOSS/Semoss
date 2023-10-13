package prerna.reactor.algorithms.xray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetLocalSchemaReactor extends AbstractReactor {
	public GetLocalSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (engineId == null) {
			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DATABASE.getKey());
		}
		engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);
		IDatabaseEngine engine = Utility.getDatabase(engineId);
		Set<String> concepts = MasterDatabaseUtility.getConceptsWithinDatabaseRDBMS(engineId);

		// tablename: [{name, type}]
		HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>();
		for (String table : concepts) {
			// ignore default concept value
			ArrayList<HashMap> allCols = new ArrayList<HashMap>();
			HashMap<String, String> colInfo = new HashMap<String, String>();
			colInfo.put("name", table);
			String dataType = MasterDatabaseUtility.getBasicDataType(engineId, table, null);;
			if (dataType != null) {
				dataType = SemossDataType.convertStringToDataType(dataType).toString();
			} else {
				dataType = SemossDataType.STRING.toString();
			}
			colInfo.put("type", dataType);
			allCols.add(colInfo);
			List<String> properties = MasterDatabaseUtility.getSpecificConceptProperties(table, engineId);
			for (String prop : properties) {
				HashMap<String, String> propInfo = new HashMap<String, String>();
				propInfo.put("name", prop);
				dataType = MasterDatabaseUtility.getBasicDataType(engineId, prop, table);;
				if (dataType != null) {
					if(dataType.contains("TYPE:")) {
						dataType = dataType.replace("TYPE:", "");
					}
					dataType = SemossDataType.convertStringToDataType(dataType).toString();
				} else {
					dataType = SemossDataType.STRING.toString();
				}
				propInfo.put("type", dataType);
				allCols.add(propInfo);
			}
			tableDetails.put(table, allCols);
		}

		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("databaseName", engine.getEngineName());
		ret.put("tables", tableDetails);

		return new NounMetadata(ret, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
