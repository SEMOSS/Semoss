package prerna.engine.impl.vector;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class VectorDatabaseQueryReactor extends AbstractReactor {

	public VectorDatabaseQueryReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey(), 
				ReactorKeysEnum.COMMAND.getKey(), 
				ReactorKeysEnum.LIMIT.getKey()};
		this.keyRequired = new int[] {1, 1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this model");
		}
		
		String question = Utility.decodeURIComponent(this.keyValue.get(this.keysToGet[1]));
		String limit = this.keyValue.get(this.keysToGet[2]);
		
		IVectorDatabaseEngine eng = Utility.getVectorDatabase(engineId);

		Object output = eng.nearestNeighbor(question, limit);
		return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);	
	}
}
