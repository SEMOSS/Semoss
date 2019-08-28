package prerna.sablecc2.reactor.app.metaeditor;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ReloadAppOwlReactor extends AbstractMetaEditorReactor {

	/*
	 * This class is when you make local changes to the OWL file and want to 
	 * reload the owl for the engine with that change
	 * 
	 * This is because the RC on the engine OWL will not be synchronized
	 */
	
	public ReloadAppOwlReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = testAppId(appId, true);
		
		IEngine engine = Utility.getEngine(appId);
		RDFFileSesameEngine oldOwlEngine =  engine.getBaseDataEngine();
		// load a new owl engine from the file
		RDFFileSesameEngine updatedOwlEngine = loadOwlEngineFile(appId);
		// replace
		engine.setBaseDataEngine(updatedOwlEngine);
		// close the old engine
		oldOwlEngine.closeDB();
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully reloaded app owl", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
}
