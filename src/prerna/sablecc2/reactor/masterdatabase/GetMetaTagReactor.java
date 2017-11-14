package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class GetMetaTagReactor extends AbstractMetaDBReactor {
	/**
	 * This reactor gets a tag in the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	@Override
	public NounMetadata execute() {
		
		//retrieve the inputs
		String engineName = getEngine();
		String concept = getConcept();
		
		//add the description
		String tagList = MasterDatabaseUtility.getMetadataValue(engineName, concept, Constants.TAG);
		return new NounMetadata(tagList.split(TAG_DELIMITER), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}
}
