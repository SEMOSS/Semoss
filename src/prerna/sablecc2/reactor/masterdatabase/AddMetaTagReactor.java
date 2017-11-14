package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.AddToMasterDB;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class AddMetaTagReactor extends  AbstractMetaDBReactor {
	/**
	 * This reactor add tags to the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the tags to be added
	 * 
	 *  creates the following row in the concept metadata table
	 *  id, tag, tag1:::tag2:::tag3:::...
	 *  
	 */
	
	@Override
	public NounMetadata execute() {
		String engine = getEngine();
		String concept = getConcept();
		List<String> values = getValues();
		String tagList = "";
		for(String tag: values) {
			tagList += tag + VALUE_DELIMITER;
		}
		AddToMasterDB master = new AddToMasterDB();
		boolean success = master.addMetadata(engine, concept, Constants.TAG, tagList);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

}
