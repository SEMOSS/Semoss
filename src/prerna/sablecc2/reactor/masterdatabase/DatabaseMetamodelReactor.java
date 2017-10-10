package prerna.sablecc2.reactor.masterdatabase;

import java.util.Map;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class DatabaseMetamodelReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		GenRowStruct eGrs = this.store.getNoun("engine");
		if(eGrs == null) {
			throw new IllegalArgumentException("Need to define the engine to get the concepts from");
		}
		if(eGrs.size() > 1) {
			throw new IllegalArgumentException("Can only define one engine within this call");
		}
		String engineName = eGrs.get(0).toString();
		Map<String, Object[]> metamodelObject = MasterDatabaseUtility.getMetamodelRDBMS(engineName);
		return new NounMetadata(metamodelObject, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_METAMODEL);
	}

}
