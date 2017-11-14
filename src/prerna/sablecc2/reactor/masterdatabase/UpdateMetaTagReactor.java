package prerna.sablecc2.reactor.masterdatabase;

import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class UpdateMetaTagReactor extends AbstractMetaDBReactor {
	/**
	 * This reactor updates a tag in the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the tag to be updated
	 */
	
	@Override
	public NounMetadata execute() {
		
		//retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();
		Vector<String> tags = getNewValue();
		String tagList = "";
		for(String tag: tags) {
			tagList += tag + TAG_DELIMITER;
		}
		
		//add the description
		boolean success = MasterDatabaseUtility.updateMetaValue(engine, concept, Constants.TAG, tagList);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
}
