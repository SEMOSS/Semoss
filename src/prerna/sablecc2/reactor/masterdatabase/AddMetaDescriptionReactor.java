package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AddMetaDescriptionReactor extends AbstractMetaDBReactor {
	
	/**
	 * This reactor adds a description to the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the description to be added
	 */
	
	public AddMetaDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), DESCRIPTION};
	}
	
	@Override
	public NounMetadata execute() {
		String engineId = getEngineId();
		engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		String concept = getConcept();
		String description = getDescription();
		AddToMasterDB master = new AddToMasterDB();
		boolean success = master.addMetadata(engineId, concept, Constants.DESCRIPTION, description);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

	
	
}
