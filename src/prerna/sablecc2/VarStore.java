package prerna.sablecc2;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.NounMetadata;

public class VarStore {

	private Map<String, NounMetadata> varMap;
	
	public VarStore() {
		varMap = new HashMap<>();
	}
	
	public void addVariable(String varName, NounMetadata variable) {
		varName = cleanVarName(varName);
		varMap.put(varName, variable);
	}
	
	public NounMetadata getVariable(String varName) {
		varName = cleanVarName(varName);
		return varMap.get(varName);
	}
	
	public boolean hasVariable(String varName) {
		varName = cleanVarName(varName);
		return varMap.containsKey(varName);
	}
	
	public NounMetadata removeVariable(String varName) {
		varName = cleanVarName(varName);
		return varMap.remove(varName);
	}
	
	public void clearVarMap() {
		this.varMap.clear();
	}
	
	private String cleanVarName(String varName) {
		return varName.trim().toUpperCase();
	}
}
