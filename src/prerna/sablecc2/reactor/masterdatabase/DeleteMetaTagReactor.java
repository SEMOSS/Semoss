package prerna.sablecc2.reactor.masterdatabase;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class DeleteMetaTagReactor extends AbstractMetaDBReactor {
	
	/**
	 * This reactor deletes a tag in the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	@Override
	public NounMetadata execute() {
		
		//retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();
		String tag = getTag();
		
		//add the description
		//TODO
//		boolean success = MasterDatabaseUtility.deleteTag(engine, concept);
		
		return new NounMetadata(null, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
}
