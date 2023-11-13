package prerna.reactor.database.metaeditor.relationships;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class AddOwlRelationshipReactor extends AbstractMetaEditorReactor {

	private static final Logger classLogger = LogManager.getLogger(AddOwlRelationshipReactor.class);

	private static final String CLASS_NAME = AddOwlRelationshipReactor.class.getName();
	
	/*
	 * This class assumes that the start table, start column, end table, and end column have already been defined
	 */
	
	public AddOwlRelationshipReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), "startT", "endT", "startC", "endC", STORE_VALUES_FRAME};
	}
	
	@Override
	public NounMetadata execute() {
		String databaseId = getAppInput();
		// we may have the alias
		databaseId = testDatabaseId(databaseId, true);
		
		// get tables
		List<String> startTList = getValues(this.keysToGet[1], 1);
		List<String> endTList = getValues(this.keysToGet[2], 2);

		// get columns if exist
		List<String> startCList = getValues(this.keysToGet[3], 3);
		List<String> endCList = getValues(this.keysToGet[4], 4);

		int size = startTList.size();
		if(size != endTList.size() && size != startCList.size() && size != endCList.size()) {
			throw new IllegalArgumentException("Input values are not the same size");
		}
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		
		try(WriteOWLEngine owlEngine = database.getOWLEngineFactory().getWriteOWL()) {
			for(int i = 0; i < size; i++) {
				String startT = startTList.get(i);
				String endT = endTList.get(i);
				String startC = startCList.get(i);
				String endC = endCList.get(i);
				// define the rel
				String rel = startT + "." + startC + "." + endT + "." + endC;
				// add the relationship
				owlEngine.addRelation(startT, endT, rel);
			}
			
			owlEngine.commit();
			
			try {
				owlEngine.export();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
				noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to add the relationships defined", 
						PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				return noun;
			}
			EngineSyncUtility.clearEngineCache(databaseId);
			ClusterUtil.pushOwl(databaseId);
	
			// store user inputed values
			storeUserInputs(getLogger(CLASS_NAME), startTList, startCList, endTList, endCList, "added");
		
		} catch (IOException | InterruptedException e1) {
			classLogger.error(Constants.STACKTRACE, e1);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to modify the OWL", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully adding relationships", 
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	
	public String getAppInput() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {	
			return grs.get(0).toString();
		}
		
		// if we got to this point
		// it better be single values
		organizeKeys();
		return this.keyValue.get(this.keysToGet[0]);
	}
	
	public List<String> getValues(String keyToGet, int index) {
		List<String> values = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(keyToGet);
		if (grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				values.add(grs.get(i).toString());
			}
			return values;
		}
		
		// if we got to this point
		// it better be single values
		organizeKeys();
		values.add(this.keyValue.get(keyToGet));
		return values;
	}
}
