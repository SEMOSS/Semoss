package prerna.reactor.insights.recipemanagement;

import java.util.List;
import java.util.Vector;

import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetInsightParamValueReactor extends AbstractInsightParameterReactor {

	private static final String PARAM_NAME = "paramName";
	private static final String PARAM_VALUE = "paramValue";
	
	public SetInsightParamValueReactor() {
		this.keysToGet = new String[] {PARAM_NAME, PARAM_VALUE};
	}
	
	@Override
	public NounMetadata execute() {
		String paramName = getParamName();
		List<Object> paramValues = getParamValue();
		if(paramValues.isEmpty()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		Object setValue = paramValues;
		if(paramValues.size() == 1) {
			setValue = paramValues.get(0);
		}
		
		// fill this in for the param struct
		String variableName = VarStore.PARAM_STRUCT_PREFIX + paramName;
		NounMetadata paramNoun = this.insight.getVarStore().get(variableName);
		if(paramNoun == null ||
				(paramNoun.getNounType() != PixelDataType.PARAM_STRUCT) ) {
			// will make my own param struct
			// and also fill in
			ParamStruct p = new ParamStruct();
			p.setParamName(paramName);
			ParamStructDetails d = new ParamStructDetails();
			d.setCurrentValue(setValue);
			p.addParamStructDetails(d);
			// store in the insight
			paramNoun = new NounMetadata(p, PixelDataType.PARAM_STRUCT);
			this.insight.getVarStore().put(variableName, paramNoun);
			// still just return false to denote it wasn't pre-existing
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		ParamStruct pStruct = (ParamStruct) paramNoun.getValue();
		List<ParamStructDetails> details = pStruct.getDetailsList();
		for(ParamStructDetails detail : details) {
			detail.setCurrentValue(setValue);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	/**
	 * Get the param name
	 * @return
	 */
	private String getParamName() {
		GenRowStruct grs = this.store.getNoun(PARAM_NAME);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		if(!this.curRow.isEmpty()) {
			return this.curRow.get(0).toString();
		}
		
		throw new IllegalArgumentException("Must pass in the parameter name");
	}
	
	/**
	 * Get the param values passed in
	 * @return
	 */
	private List<Object> getParamValue() {
		List<Object> values = new Vector<Object>();
		GenRowStruct paramValue = this.store.getNoun(PARAM_VALUE);
		if(paramValue != null && !paramValue.isEmpty()) {
			for(int i = 0; i < paramValue.size(); i++) {
				values.add(paramValue.getNoun(i).getValue());
			}
		}
		
		return values;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(PARAM_NAME)) {
			return "The name of the param";
		} else if(key.equals(PARAM_VALUE)) {
			return "The value of the param";
		}
		return super.getDescriptionForKey(key);
	}
	
}
