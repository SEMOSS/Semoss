package prerna.reactor.model;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.modelinference.ModelInferenceQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ModelReactor extends AbstractQueryStructReactor {
	
	public ModelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.MODEL.getKey()};
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		this.organizeKeys();
		
		String modelId = this.keyValue.get(ReactorKeysEnum.MODEL.getKey());
		
		// we may have the alias
		modelId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), modelId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), modelId)) {
			throw new IllegalArgumentException("Model " + modelId + " does not exist or user does not have access to it");
		}
		
		this.qs.setEngineId(modelId);
		this.qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		
		return this.qs;
	}
	
	//initialize the reactor with its necessary inputs
	@Override
	protected void init() {
		// this will happen when we have an explicit querystruct
		// or one result piped a query struct to the current reactor
		GenRowStruct qsInputParams = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.getKey());
		if(qsInputParams != null) {
			int numInputs = qsInputParams.size();
			for(int inputIdx = 0; inputIdx < numInputs; inputIdx++) {
				NounMetadata qsNoun = (NounMetadata)qsInputParams.getNoun(inputIdx);
				AbstractQueryStruct qs = (AbstractQueryStruct) qsNoun.getValue();
				mergeQueryStruct(qs);
			}
		}
		
		// if it is not piped
		// but there is a query struct within a query struct
		// the specific instance of the reactor will handle those types of merges
		// example
		// selector ( studio , sum(mb) ) 
		// the selector reactor will handle putting the studio and the sum(mb)
		
		if(this.qs == null) {
			this.qs = new ModelInferenceQueryStruct();
		}
	}
}
