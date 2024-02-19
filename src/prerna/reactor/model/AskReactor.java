package prerna.reactor.model;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.query.querystruct.modelinference.ModelInferenceQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AskReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AskReactor.class);

	public AskReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		ModelInferenceQueryStruct qs = getQueryStruct();
					
		IModelEngine model = Utility.getModel(qs.getEngineId());

		String question = Utility.decodeURIComponent(this.keyValue.get(ReactorKeysEnum.COMMAND.getKey()));

		String context = qs.getContext();
		Map<String, Object> hyperParameters = qs.getHyperParameters();
		Map<String, Object> output = model.ask(question, context, this.insight, hyperParameters);
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	private ModelInferenceQueryStruct getQueryStruct() {
		NounMetadata noun = null;
		ModelInferenceQueryStruct qs = null;

		GenRowStruct grsQs = this.store.getNoun(PixelDataType.QUERY_STRUCT.getKey());
		//if we don't have tasks in the curRow, check if it exists in genrow under the qs key
		if(grsQs != null && !grsQs.isEmpty()) {
			noun = grsQs.getNoun(0);
			qs = (ModelInferenceQueryStruct) noun.getValue();
		} else {
			List<NounMetadata> qsList = this.curRow.getNounsOfType(PixelDataType.QUERY_STRUCT);
			if(qsList != null && !qsList.isEmpty()) {
				noun = qsList.get(0);
				qs = (ModelInferenceQueryStruct) noun.getValue();
			}
		}
		
		if (qs == null) {
			throw new IllegalArgumentException("Please create a valid query struct.");
		}
		
		return qs;
	}
}
