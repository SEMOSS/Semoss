package prerna.sablecc2.reactor.masterdatabase;

import java.util.Vector;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class UpdateMetaLinkReactor extends  AbstractMetaDBReactor {
	/**
	 * This reactor updates links in the concept metadata table 
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the links to be updated
	 * 
	 * 	updates the following row in the concept metadata table
	 *  id, link, link1:::link2:::link3:::...
	 */
	@Override
	public NounMetadata execute() {
		String engine = getEngine();
		String concept = getConcept();
		Vector<String> values = getValues();
		String hyperLinkList = "";
		for(String link: values) {
			hyperLinkList += link + VALUE_DELIMITER;
		}
		boolean success = MasterDatabaseUtility.updateMetaValue(engine, concept, Constants.TAG, hyperLinkList);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

}
