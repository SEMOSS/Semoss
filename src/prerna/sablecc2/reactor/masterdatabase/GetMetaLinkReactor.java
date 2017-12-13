package prerna.sablecc2.reactor.masterdatabase;

import java.util.ArrayList;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Constants;

public class GetMetaLinkReactor extends  AbstractMetaDBReactor {

	/**
	 * This reactor gets the links from the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	public GetMetaLinkReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String engineName = getEngine();
		String concept = getConcept();
		String hyperLink = MasterDatabaseUtility.getMetadataValue(engineName, concept, Constants.LINK);
		ArrayList<String> list = new ArrayList<String>();
		if (hyperLink != null) {
			for (String link : hyperLink.split(VALUE_DELIMITER)) {
				if (link.length() > 0) {
					list.add(link);
				}
			}
		}
		return new NounMetadata(list, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}

}
