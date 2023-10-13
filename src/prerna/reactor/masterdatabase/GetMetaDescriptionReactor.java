package prerna.reactor.masterdatabase;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GetMetaDescriptionReactor extends AbstractMetaDBReactor {
	
	/**
	 * This reactor gets the description from the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 */
	
	public GetMetaDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String engineId = getEngineId();
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("App does not exist or user does not have access to edit database");
		}
		
		String concept = getConcept();
		String description = MasterDatabaseUtility.getMetadataValue(engineId, concept, Constants.DESCRIPTION);
		String output = "";
		if(description != null) {
			output = description;
		}
		return new NounMetadata(output, PixelDataType.BOOLEAN, PixelOperationType.CODE_EXECUTION);
	}
}
