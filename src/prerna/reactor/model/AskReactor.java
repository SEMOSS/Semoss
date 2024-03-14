package prerna.reactor.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.AbstractModelEngine;
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
	private static final String FULL_PROMPT = "fullPrompt";
	
	public AskReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey(), FULL_PROMPT};
		this.keyRequired = new int[] {0 , 0};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		ModelInferenceQueryStruct qs = getQueryStruct();
					
		IModelEngine model = Utility.getModel(qs.getEngineId());

		String question = this.keyValue.get(ReactorKeysEnum.COMMAND.getKey());
		String context = qs.getContext();
		Map<String, Object> hyperParameters = qs.getHyperParameters();
		Object fullPrompt = getFullPrompt();
		
		if (question == null && fullPrompt == null) {
			throw new IllegalArgumentException("Please provide either an input using either commnad or fullPrompt.");
		}
		
		if (fullPrompt != null) {
			if (hyperParameters == null) {
				hyperParameters = new HashMap<>();
			}
			hyperParameters.put(AbstractModelEngine.FULL_PROMPT, fullPrompt);
		} else {
			question = Utility.decodeURIComponent(question);
		}
		
		Map<String, Object> output = model.ask(question, context, this.insight, hyperParameters).toMap();
		return new NounMetadata(output, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
	
	private Object getFullPrompt() {
		GenRowStruct grs = this.store.getNoun(FULL_PROMPT);
		if (grs != null) {
			
			NounMetadata firstInput = grs.getNoun(0);
			if (firstInput.getValue() instanceof String) {
				return firstInput.getValue();
			}
			
			return grs.getAllValues();
		}
		
		return null;
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
	
	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FULL_PROMPT)) {
			return "The exact input that will be sent directly to a model engine. This requires a user to know the prompt structure of the large language model and keep track of conversation history themselves.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
