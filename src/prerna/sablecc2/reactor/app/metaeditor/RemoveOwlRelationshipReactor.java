package prerna.sablecc2.reactor.app.metaeditor;

import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.OWLER;
import prerna.util.Utility;

public class RemoveOwlRelationshipReactor extends AbstractMetaEditorReactor {

	/*
	 * This class assumes that the start table, start column, end table, and end column have already been defined
	 */
	
	public RemoveOwlRelationshipReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), "startT", "endT", "startC", "endC", "relName"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = getAppId(appId, true);
		
		// get tables
		String startT = this.keyValue.get(this.keysToGet[1]);
		String endT = this.keyValue.get(this.keysToGet[2]);

		// get columns if exist
		String startC = this.keyValue.get(this.keysToGet[3]);
		String endC = this.keyValue.get(this.keysToGet[4]);

		// get rel if defined
		String rel = this.keyValue.get(this.keysToGet[5]);
		if(rel == null) {
			if(startC != null && endC != null) {
				rel = startT + "." + startC + "." + endT + "." + endC;
			} else {
				rel = startT + "_" + endC;
			}
		}
		
		OWLER owler = getOWLER(appId);
		// set all the existing values into the OWLER
		// so that its state is updated
		IEngine engine = Utility.getEngine(appId);
		boolean isRdbms = (engine.getEngineType() == IEngine.ENGINE_TYPE.RDBMS || 
				engine.getEngineType() == IEngine.ENGINE_TYPE.IMPALA);
		setOwlerValues(engine, owler);
		
		if(isRdbms) {
			// the relation has the startC and endC
			// what I really need is the primary key for the tables
			startC = Utility.getClassName(engine.getPhysicalUriFromConceptualUri(OWLER.BASE_URI + OWLER.DEFAULT_NODE_CLASS + "/" + startT));
			endC = Utility.getClassName(engine.getPhysicalUriFromConceptualUri(OWLER.BASE_URI + OWLER.DEFAULT_NODE_CLASS + "/" + endT));
		}
		
		// add the relationship
		owler.removeRelation(startT, startC, endT, endC, rel);
		owler.commit();
		
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to remove the relationship between " + startT + " and " + endT, 
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully remove relationship between " + startT + " and " + endT, 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS_MESSAGE));
		return noun;
	}

}
