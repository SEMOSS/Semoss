package prerna.query.parsers;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class ParamStruct {

	////////////////////////////////////////////////////////
	
	/*
	 * Keeping static information in a central place around the param structs
	 */
	
	// this is based on if the JSON requires a search
	public static final Set<String> APPEND_SEARCH_PARAM_TYPES = new HashSet<>();
	static {
		APPEND_SEARCH_PARAM_TYPES.add("checklist");
		APPEND_SEARCH_PARAM_TYPES.add("dropdown");
		APPEND_SEARCH_PARAM_TYPES.add("typeahead");
	}
	
	// if we need the "quickselect" key in the JSON
	public static final Set<String> APPEND_QUICK_SELECT_TYPES = new HashSet<>();
	static {
		APPEND_QUICK_SELECT_TYPES.add("checklist");
	}
	
	// if we need the "useSelectedValues" key in the JSON
	public static final Set<String> APPEND_USE_SELECTED_VALUES_TYPES = new HashSet<>();
	static {
		APPEND_USE_SELECTED_VALUES_TYPES.add("checklist");
	}
	
	// if we need the "defaultValue" value to be an array 
	public static final Set<String> DEFAULT_VALUE_IS_ARRAY = new HashSet<>();
	static {
		DEFAULT_VALUE_IS_ARRAY.add("checklist");
	}
	
	// if we need to generate a default model type if one does not exist
	public static final Set<String> REQUIRE_MODEL_TYPES = new HashSet<>();
	static {
		REQUIRE_MODEL_TYPES.add("checklist");
		REQUIRE_MODEL_TYPES.add("dropdown");
		REQUIRE_MODEL_TYPES.add("typeahead");
	}
	
	// if, for sqs, we need the parameter to be [<pName>] or just <pName>
	public static final Set<String> PARAM_FILL_USE_ARRAY_TYPES = new HashSet<>();
	static {
		PARAM_FILL_USE_ARRAY_TYPES.add("checklist");
	}
	
	////////////////////////////////////////////////////////

	public enum FILL_TYPE {MANUAL, PIXEL, QUERY}
	
	private List<ParamStructDetails> detailsList = new Vector<>();
	
	private boolean searchable = false;
	private boolean multiple = false;
	private String paramName = null;
	// this is the model app id
	// but consolidating the key for FE
	private String appId = null;
	private String modelQuery = null;
	private Object manualChoices = null;
	private String modelDisplay = null; // need to turn this into an enum
	private String modelLabel = null; // how do you want to ask your user what to do ?
	private boolean required = true;
	private Object defaultValue = null;
	private boolean populateInAudit = true; // to include the param in audit sheet or not
	private boolean isPreApplied = false;  // to know wether its preapplied on payload

	private FILL_TYPE fillType = null;
	
	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public boolean isSearchable() {
		return searchable;
	}

	public void setSearchable(boolean searchable) {
		this.searchable = searchable;
	}

	public boolean isMultiple() {
		return multiple;
	}

	public void setMultiple(boolean multiple) {
		this.multiple = multiple;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParamName(String paramName) {
		this.paramName = paramName;
	}
	
	public String getModelAppId() {
		return appId;
	}

	public void setModelAppId(String modelAppId) {
		this.appId = modelAppId;
	}

	public String getModelQuery() {
		return modelQuery;
	}

	public void setModelQuery(String modelQuery) {
		this.modelQuery = modelQuery;
	}

	public Object getManualChoices() {
		return manualChoices;
	}

	public void setManualChoices(Object manualChoices) {
		this.manualChoices = manualChoices;
	}

	public String getModelDisplay() {
		return modelDisplay;
	}

	public void setModelDisplay(String modelDisplay) {
		this.modelDisplay = modelDisplay;
	}

	public String getModelLabel() {
		return modelLabel;
	}

	public void setModelLabel(String modelLabel) {
		this.modelLabel = modelLabel;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
	}
	
	public boolean isPopulateInAudit() {
		return populateInAudit;
	}

	public void setPopulateInAudit(boolean populateInAudit) {
		this.populateInAudit = populateInAudit;
	}

	public boolean isPreApplied() {
		return isPreApplied;
	}

	public void setPreApplied(boolean isPreApplied) {
		this.isPreApplied = isPreApplied;
	}

	public FILL_TYPE getFillType() {
		return fillType;
	}

	public void setFillType(FILL_TYPE fillType) {
		this.fillType = fillType;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for(int i = 0; i < detailsList.size(); i++) {
			if(i > 0) {
				builder.append(",");
			}
			builder.append(detailsList.get(i).toString());
		}

		return builder.toString();
	}
	
	public void addParamStructDetails(ParamStructDetails detailsStruct) {
		this.detailsList.add(detailsStruct);
	}
	
	public List<ParamStructDetails> getDetailsList() {
		return this.detailsList;
	}
	
	public void swapOptimizedIds() {
		for(ParamStructDetails detail : this.detailsList) {
			detail.swapOptimizedIds();
		}
	}
	
	/**
	 * Generate a param struct from map inputs
	 * @param mapInputs
	 * @return
	 */
	public static ParamStruct generateParamStruct(Map<String, Object> mapInputs) {
		Object defaultValue = mapInputs.get("defaultValue");
		Boolean searchable = (Boolean) mapInputs.get("searchable");
		Boolean multiple = (Boolean) mapInputs.get("multiple");
		String paramName = (String) mapInputs.get("paramName");
		String modelAppId = (String) mapInputs.get("appId");
		String modelQuery = (String) mapInputs.get("modelQuery");
		Object manualChoices = mapInputs.get("manualChoices");
		String modelDisplay = (String) mapInputs.get("modelDisplay");
		String modelLabel = (String) mapInputs.get("modelLabel");
		Boolean required = (Boolean) mapInputs.get("required");
		Boolean populateInAudit = (Boolean) mapInputs.get("populateInAudit");
		Boolean isPreApplied = (Boolean) mapInputs.get("isPreApplied");
		
		// these are enums
		String fillType = (String) mapInputs.get("fillType");
		FILL_TYPE fillT = FILL_TYPE.valueOf(fillType);
		ParamStruct pStruct = new ParamStruct();
		if(fillType != null && !fillType.isEmpty()) {
			pStruct.setFillType(fillT);
		}
		pStruct.setDefaultValue(defaultValue);
		if(searchable != null) {
			pStruct.setSearchable(searchable);
		}
		if(multiple != null) {
			pStruct.setMultiple(multiple);
		}
		pStruct.setParamName(paramName);
		pStruct.setModelAppId(modelAppId);
		pStruct.setModelQuery(modelQuery);
		pStruct.setManualChoices(manualChoices);
		pStruct.setModelDisplay(modelDisplay);
		pStruct.setModelLabel(modelLabel);
		if(required != null) {
			pStruct.setRequired(required);
		}
		if(populateInAudit != null && !populateInAudit) {
			pStruct.setPopulateInAudit(false);
		} else {
			pStruct.setPopulateInAudit(true);
		}
		if(isPreApplied != null && isPreApplied) {
			pStruct.setPreApplied(true);
		}
		
		// now need to handle the details
		List<Map<String, Object>> detailsListMap = (List<Map<String, Object>>) mapInputs.get("detailsList");
		int numDetails = detailsListMap.size();
		for(int i = 0; i < numDetails; i++) {
			pStruct.detailsList.add(ParamStructDetails.generateParamStructDetails(detailsListMap.get(i)));
		}
		
		return pStruct;
	}
}
