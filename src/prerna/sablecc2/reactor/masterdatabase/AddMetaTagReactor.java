package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class AddMetaTagReactor extends  AbstractMetaDBReactor {
	/**
	 * This reactor adds a tag to the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the tag to be added
	 */
	
	@Override
	public NounMetadata execute() {
		
		//retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();
		List<String> values = getNewValue();
		String tagList = "";
		for(String tag: values) {
			tagList += tag + TAG_DELIMITER;
		}
		
		//add the description
		AddToMasterDB master = new AddToMasterDB();
		boolean success = master.addMetadata(engine, concept, Constants.TAG, tagList);
		
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

}
