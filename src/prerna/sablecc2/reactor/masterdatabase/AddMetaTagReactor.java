package prerna.sablecc2.reactor.masterdatabase;

import java.util.Arrays;
import java.util.List;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

/**
 * This reactor add tags to the concept metadata table The inputs to the reactor
 * are: 1) the engine 2) the the concept 3) the tags to be added
 * 
 * creates the following row in the concept metadata table
 * 
 * example localConceptID, tag, tag1:::tag2:::tag3:::...
 * 
 */
public class AddMetaTagReactor extends AbstractMetaDBReactor {
	
	public AddMetaTagReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), VALUES};
	}

	@Override
	public NounMetadata execute() {
		String engineId = getEngineId();

		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		}
		
		if(!SecurityQueryUtils.getEngineIds().contains(engineId)) {
			throw new IllegalArgumentException("App id does not exist");
		}
		
		String concept = getConcept();
		List<String> values = getValues();
		String newTagList = "";
		// check if tags exist
		String oldTagList = MasterDatabaseUtility.getMetadataValue(engineId, concept, Constants.TAG);
		if (oldTagList != null) {
			String[] oldTags = oldTagList.split(VALUE_DELIMITER);
			for (String tag : values) {
				// don't allow duplicates
				if (!Arrays.asList(oldTags).contains(tag)) {
					newTagList += tag + VALUE_DELIMITER;
				}
			}
		} // case adding new tags for the first time
		else {
			oldTagList = "";
			for (String tag : values) {
				newTagList += tag + VALUE_DELIMITER;
			}
		}
		newTagList = oldTagList + newTagList;
		AddToMasterDB master = new AddToMasterDB();
		boolean success = master.addMetadata(engineId, concept, Constants.TAG, newTagList);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}

}
