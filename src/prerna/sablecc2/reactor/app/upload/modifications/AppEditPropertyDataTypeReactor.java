package prerna.sablecc2.reactor.app.upload.modifications;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineModifier;
import prerna.engine.impl.modifications.EngineModificationFactory;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.metaeditor.properties.EditOwlPropertyDataTypeReactor;
import prerna.util.Utility;

public class AppEditPropertyDataTypeReactor extends AbstractReactor {

	public AppEditPropertyDataTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(),
				ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.DATA_TYPE.getKey(),
		};
		this.keyRequired = new int[]{1, 1, 1, 1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
		String engineId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
				throw new IllegalArgumentException("App " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("App " + engineId + " does not exist");
			}
		}
		
		String table = this.keyValue.get(this.keysToGet[1]);
		String newColumn = this.keyValue.get(this.keysToGet[2]);
		String newColType = this.keyValue.get(this.keysToGet[3]);
		
		IEngine engine = Utility.getEngine(engineId);
		
		// keep track of existing data type in case something goes wrong
		
		// update the owl for any engine
		// we will just update the 
		// the additional data types + description + new column type
		// are handled and used by this reactor
		EditOwlPropertyDataTypeReactor owlUpdater = new EditOwlPropertyDataTypeReactor();
		owlUpdater.setInsight(this.insight);
		owlUpdater.setNounStore(this.store);
		owlUpdater.execute();

		IEngineModifier modifier = EngineModificationFactory.getEngineModifier(engine);
		if(modifier == null) {
			throw new IllegalArgumentException("This type of data modification has not been implemented for this database type");
		}
		try {
			modifier.addProperty(table, newColumn, newColType);
		} catch (Exception e) {
			// an error occurred here, so we need to revert our change from the OWL
//			try {
//				EditOwlPropertyDataTypeReactor owlRemover = new EditOwlPropertyDataTypeReactor();
//				owlRemover.setInsight(this.insight);
//				owlRemover.setNounStore(this.store);
//				owlRemover.execute();
//			} catch(Exception e2) {
//				e2.printStackTrace();
//			}
			
			throw new IllegalArgumentException("Error occured to alter the table. Error returned from driver: " + e.getMessage(), e);
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully modified data type of property"));
		return noun;
	}
}
