package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import prerna.auth.SecurityQueryUtils;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class AddMetaLinkReactor extends  AbstractMetaDBReactor  {
	/**
	 * This reactor add links to the concept metadata table
	 * The inputs to the reactor are: 
	 * 1) the engine
	 * 2) the the concept
	 * 3) the links to be added
	 * 
	 *  creates the following row in the concept metadata table
	 *  id, link, link1:::link2:::link3:::...
	 *  
	 */
	
	public AddMetaLinkReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(), VALUES};
	}
	
	@Override
	public NounMetadata execute() {
		String engineId = getEngineId();

		if(this.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityQueryUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
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
		String hyperLinkList = "";
		for(String link: values) {
			hyperLinkList += link + VALUE_DELIMITER;
		}
		AddToMasterDB master = new AddToMasterDB();
		boolean success = master.addMetadata(engineId, concept, Constants.LINK, hyperLinkList);
		return new NounMetadata(success, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}

}
