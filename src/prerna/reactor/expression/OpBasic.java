package prerna.reactor.expression;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class OpBasic extends OpReactor {

	protected String operation;
	protected boolean allIntValue = true;
	protected NounMetadata[] nouns;
	/*
	 * This class is to be extended for basic math operations
	 * To deal with string inputs that need to be evaluated
	 * on the frame
	 * 
	 * TODO: stop casting everything to h2frame
	 * 		make generic expression class that uses
	 * 		existing classes
	 */
	
	@Override
	public NounMetadata execute() {
		this.nouns = getValues();
		Object[] values = evaluateNouns(this.nouns);
		NounMetadata result = evaluate(values);
		return result;
	}
	
	protected abstract NounMetadata evaluate(Object[] values);
	
	protected Object[] evaluateNouns(NounMetadata[] nouns) {
		Object[] evaluatedNouns = new Object[nouns.length];
		for(int i = 0; i < nouns.length; i++) {
			NounMetadata val = nouns[i];
			evaluatedNouns[i] = evaluateNoun(val);
		}
		return evaluatedNouns;
	}
	
	protected Object evaluateNoun(NounMetadata val) {
		Object obj = null;
		PixelDataType valType = val.getNounType();
		if(valType == PixelDataType.CONST_DECIMAL) {
			this.allIntValue = false;
			obj = ((Number) val.getValue()).doubleValue();
		} else if(valType == PixelDataType.CONST_INT) {
			obj = ((Number) val.getValue()).intValue(); 
		} else if(valType == PixelDataType.VECTOR) {
			List<NounMetadata> nouns = (List<NounMetadata>) val.getValue();
			Object[] objArray = new Object[nouns.size()];
			for(int i = 0; i < nouns.size(); i++) {
				objArray[i] = evaluateNoun(nouns.get(i));
			}
			obj = objArray;
		} 
		// sometimes this can still be an iterator which is fine
		else if(valType == PixelDataType.FORMATTED_DATA_SET
				&& val.getValue() instanceof Map) {
			Map<String, Object> taskMap = (Map<String, Object>) val.getValue();
			Map<String, Object> dataMap = (Map<String, Object>) taskMap.get("data");
			List<Object[]> values = (List<Object[]>) dataMap.get("values");
			if(values.size() != 1) {
				throw new IllegalArgumentException("Dataset must be a single value");
			}
			Object[] row = values.get(0);
			if(row.length != 1) {
				throw new IllegalArgumentException("Dataset must be a single value");
			}
			obj = row[0];
		} else {
			obj = val.getValue();
		}
		
		return obj;
	}
}
