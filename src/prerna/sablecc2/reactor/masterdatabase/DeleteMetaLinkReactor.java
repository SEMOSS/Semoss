package prerna.sablecc2.reactor.masterdatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.Constants;

public class DeleteMetaLinkReactor extends  AbstractMetaDBReactor {

	/**
	 * This reactor deletes links in the metadata
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) links to delete
	 */
	@Override
	public NounMetadata execute() {
		// retrieve the inputs
		String engine = getEngine();
		String concept = getConcept();
		List<String> valuesToDelete = getValues();

		// get links from concept metadata table
		String linkString = MasterDatabaseUtility.getMetadataValue(engine, concept, Constants.LINK);
		ArrayList<String> linkList = new ArrayList<String>(Arrays.asList(linkString.split(VALUE_DELIMITER)));

		// remove values
		for (String link : valuesToDelete) {
			linkList.remove(link);
		}
		String updatedLinks = "";
		for (String link : linkList) {
			updatedLinks += link + VALUE_DELIMITER;
		}
		boolean deleteUpdate = MasterDatabaseUtility.updateMetaValue(engine, concept, Constants.LINK, updatedLinks);
		return new NounMetadata(deleteUpdate, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}

}
