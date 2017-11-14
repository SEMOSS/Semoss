package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class DeleteMetaDescriptionReactor extends AbstractMetaDBReactor {

	/**
	 * This reactor deletes a description in the metadata The inputs to the
	 * reactor are: 1) the engine 2) the the concept
	 */

	@Override
	public NounMetadata execute() {

		// retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();

		// add the description
		boolean success = MasterDatabaseUtility.deleteMetaValue(engine, concept, Constants.DESCRIPTION);
		return new NounMetadata(null, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

}
