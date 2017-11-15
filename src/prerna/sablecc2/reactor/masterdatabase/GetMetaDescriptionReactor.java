package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class GetMetaDescriptionReactor extends AbstractMetaDBReactor {
	
	/**
	 * This reactor gets the description from the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	@Override
	public NounMetadata execute() {
		String engineName = getEngine();
		String concept = getConcept();
		List<String> description = MasterDatabaseUtility.getMetadataValue(engineName, concept, Constants.DESCRIPTION);
		return new NounMetadata(description, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

	
	
	
}
