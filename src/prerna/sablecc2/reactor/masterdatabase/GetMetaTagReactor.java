package prerna.sablecc2.reactor.masterdatabase;

import java.util.ArrayList;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetMetaTagReactor extends AbstractMetaDBReactor {
	/**
	 * This reactor gets the tags from the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	public GetMetaTagReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String engineId = getEngineId();
		
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		}
		
		if(!SecurityQueryUtils.getEngineIds().contains(engineId)) {
			throw new IllegalArgumentException("App id does not exist");
		}
		
		String concept = getConcept();
		String tagList = MasterDatabaseUtility.getMetadataValue(engineId, concept, Constants.TAG);
		ArrayList<String> list = new ArrayList<String>();
		if (tagList != null) {
			for (String tag : tagList.split(VALUE_DELIMITER)) {
				if (tag.length() > 0) {
					list.add(tag);
				}
			}
		}
		return new NounMetadata(list, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
	}
}
