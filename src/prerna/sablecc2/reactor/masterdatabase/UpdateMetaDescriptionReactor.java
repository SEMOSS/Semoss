package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class UpdateMetaDescriptionReactor extends AbstractMetaDBReactor {

	/**
	 * This reactor updates a description to the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the description to be updated
	 */
	
	@Override
	public NounMetadata execute() {
		
		//retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();
		String description = getDescription();
		
		// add the description
		boolean success = MasterDatabaseUtility.updateMetaValue(engine, concept, Constants.DESCRIPTION, description);
		
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
}
