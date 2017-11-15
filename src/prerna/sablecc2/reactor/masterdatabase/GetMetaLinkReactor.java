package prerna.sablecc2.reactor.masterdatabase;

import java.util.ArrayList;
import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class GetMetaLinkReactor extends  AbstractMetaDBReactor {

	/**
	 * This reactor gets the links from the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	@Override
	public NounMetadata execute() {
		String engineName = getEngine();
		String concept = getConcept();
		List<String> hyperLink = MasterDatabaseUtility.getMetadataValue(engineName, concept, Constants.LINK);
		ArrayList<ArrayList<String>> history = new ArrayList<ArrayList<String>>();
		for(String hyperString: hyperLink) {
			ArrayList<String> list = new ArrayList<String>();
			for(String link: hyperString.split(VALUE_DELIMITER)) {
				if(link.length() > 0) {
					list.add(link);
				}
			}
			history.add(list);
		}
		return new NounMetadata(history, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
