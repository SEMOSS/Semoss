package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class UpdateMetaDescriptionReactor extends AbstractMetaDBReactor {

	/**
	 * This reactor updates the description in the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the new description to be updated
	 */
	
	@Override
	public NounMetadata execute() {
		String engine = getEngine();
		String concept = getConcept();
		String description = getDescription();
		boolean success = MasterDatabaseUtility.updateMetaValue(engine, concept, Constants.DESCRIPTION, description);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
}
