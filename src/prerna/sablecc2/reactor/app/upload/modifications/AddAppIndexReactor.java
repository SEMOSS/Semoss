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
import prerna.sablecc2.reactor.app.metaeditor.properties.RemoveOwlPropertyReactor;
import prerna.util.Utility;

public class AddAppIndexReactor extends AbstractReactor {

	private static final String FORCE_INDEX = "force";
	
	public AddAppIndexReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(),
				ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(),
				ReactorKeysEnum.CREATE_INDEX.getKey(),
				FORCE_INDEX
		};
		this.keyRequired = new int[]{1, 1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
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
		String column = this.keyValue.get(this.keysToGet[2]);
		String indexName = this.keyValue.get(this.keysToGet[3]);
		if(indexName == null) {
			indexName = table.toUpperCase() + "_" + column.toUpperCase() + "_INDEX";
		}
		boolean forceIndex = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]));
		
		IEngine engine = Utility.getEngine(engineId);
		IEngineModifier modifier = EngineModificationFactory.getEngineModifier(engine);
		if(modifier == null) {
			throw new IllegalArgumentException("This type of data modification has not been implemented for this database type");
		}
		try {
			modifier.addIndex(table, column, indexName, forceIndex);
		} catch (Exception e) {
			// an error occurred here, so we need to delete from the OWL
			try {
				RemoveOwlPropertyReactor owlRemover = new RemoveOwlPropertyReactor();
				owlRemover.setInsight(this.insight);
				owlRemover.setNounStore(this.store);
				owlRemover.execute();
			} catch(Exception e2) {
				e2.printStackTrace();
			}
			
			throw new IllegalArgumentException("Error occured to alter the table. Error returned from driver: " + e.getMessage(), e);
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added index"));
		return noun;
	}
	
}
