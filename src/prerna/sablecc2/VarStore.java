package prerna.sablecc2;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.IReactor;

public class VarStore {

	private Map<String, NounMetadata> varMap;
	
	public VarStore() {
		varMap = new HashMap<>();
	}
	
	public void addVariable(String varName, NounMetadata variable) {
		varName = cleanVarName(varName);
		if(variable.getNounName() == PkslDataTypes.COLUMN) {
			if(varName.equalsIgnoreCase(variable.getValue().toString())) {
				System.out.println("Creating a loop here");
				return;
			}
		}
		varMap.put(varName, variable);
	}
	
	public NounMetadata getVariable(String varName) {
		varName = cleanVarName(varName);
		return varMap.get(varName);
	}
	
	public NounMetadata getVariableValue(String varName) {
		varName = cleanVarName(varName);
		NounMetadata valueNoun = varMap.get(varName);
		if(valueNoun != null) {
			PkslDataTypes valType = valueNoun.getNounName();
			if(valType == PkslDataTypes.COLUMN) {
				String valName = valueNoun.getValue().toString();
				// got to make sure it is not a variable
				// pointing to another variable
				if(hasVariable(valName)) {
					return getVariableValue(valName);
				}
			}
			else if(valType == PkslDataTypes.LAMBDA) {
				NounMetadata retNoun = ((IReactor) valueNoun.getValue()).execute();
				return retNoun;
			}
		} else if(!varName.equalsIgnoreCase("$RESULT")) {
			System.out.println("Not Found!!!  "+varName);
		}
		// once we are done with the whole recursive
		// part above, just return the noun
		return valueNoun;
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
	
	public Set<String> getVariables() {
		return varMap.keySet();
	}
}
