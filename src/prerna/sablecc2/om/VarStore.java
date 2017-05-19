package prerna.sablecc2.om;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.querystruct.QueryStruct2;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.IReactor;

public class VarStore implements InMemStore<String, NounMetadata> {

	private Map<String, NounMetadata> varMap;
	
	public VarStore() {
		varMap = new HashMap<>();
	}
	
	@Override
	public void put(String varName, NounMetadata variable) {
//		varName = cleanVarName(varName);
		if(variable.getNounName() == PkslDataTypes.COLUMN) {
			if(varName.equalsIgnoreCase(variable.getValue().toString())) {
				return;
			}
		}
		varMap.put(varName, variable);
	}
	
	@Override
	public NounMetadata get(String varName) {
//		varName = cleanVarName(varName);
		return varMap.get(varName);
	}
	
	@Override
	public NounMetadata getEvaluatedValue(String varName) {
//		varName = cleanVarName(varName);
		NounMetadata valueNoun = varMap.get(varName);
		if(valueNoun != null) {
			PkslDataTypes valType = valueNoun.getNounName();
			if(valType == PkslDataTypes.COLUMN) {
				String valName = valueNoun.getValue().toString();
				// got to make sure it is not a variable
				// pointing to another variable
				if(containsKey(valName)) {
					return getEvaluatedValue(valName);
				}
			}
			else if(valType == PkslDataTypes.LAMBDA) {
				NounMetadata retNoun = ((IReactor) valueNoun.getValue()).execute();
				return retNoun;
			}
		} else if(!varName.equalsIgnoreCase("$RESULT")) {
			
		}
		// once we are done with the whole recursive
		// part above, just return the noun
		return valueNoun;
	}
	
	@Override
	public boolean containsKey(String varName) {
//		varName = cleanVarName(varName);
		return varMap.containsKey(varName);
	}
	
	@Override
	public NounMetadata remove(String varName) {
//		varName = cleanVarName(varName);
		return varMap.remove(varName);
	}
	
	@Override
	public void clear() {
		this.varMap.clear();
	}
	
	@Override
	public Iterator<IHeadersDataRow> getIterator() {
		return null;
	}

	@Override
	public Iterator<IHeadersDataRow> getIterator(QueryStruct2 qs) {
		return null;
	}

	@Override
	public Set<String> getKeys() {
		return varMap.keySet();
	}
	
//	private String cleanVarName(String varName) {
//		return varName.trim().toUpperCase();
//	}
}
