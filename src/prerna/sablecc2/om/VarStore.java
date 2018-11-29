package prerna.sablecc2.om;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.IReactor;
import prerna.sablecc2.reactor.storage.MapHeaderDataRowIterator;

public class VarStore implements InMemStore<String, NounMetadata> {

	private Map<String, NounMetadata> varMap;
	
	public VarStore() {
		varMap = new HashMap<>();
	}
	
	@Override
	public void put(String varName, NounMetadata variable) {
		varName = cleanVarName(varName);
		if(variable.getNounType() == PixelDataType.COLUMN) {
			if(varName.equals(variable.getValue().toString())) {
				return;
			}
		}
		varMap.put(varName, variable);
	}
	
	@Override
	public NounMetadata get(String varName) {
		varName = cleanVarName(varName);
		return varMap.get(varName);
	}
	
	@Override
	public NounMetadata getEvaluatedValue(String varName) {
		varName = cleanVarName(varName);
		NounMetadata valueNoun = varMap.get(varName);
		if(valueNoun != null) {
			PixelDataType valType = valueNoun.getNounType();
			if(valType == PixelDataType.COLUMN) {
				String valName = valueNoun.getValue().toString();
				// got to make sure it is not a variable
				// pointing to another variable
				if(containsKey(valName)) {
					return getEvaluatedValue(valName);
				}
			}
			else if(valType == PixelDataType.LAMBDA) {
				NounMetadata retNoun = ((IReactor) valueNoun.getValue()).execute();
				return retNoun;
			}
		}
		// once we are done with the whole recursive
		// part above, just return the noun
		return valueNoun;
	}
	
	@Override
	public boolean containsKey(String varName) {
		varName = cleanVarName(varName);
		return varMap.containsKey(varName);
	}
	
	@Override
	public NounMetadata remove(String varName) {
		return varMap.remove(varName);
	}
	
	/**
	 * Remove all keys 
	 * @param keys
	 */
	public void removeAll(Collection<String> keys) {
		this.varMap.keySet().removeAll(keys);
	}
	
	@Override
	public void clear() {
		this.varMap.clear();
	}
	
	@Override
	public Iterator<IHeadersDataRow> getIterator() {
		return new MapHeaderDataRowIterator(this);
	}

	@Override
	public Iterator<IHeadersDataRow> getIterator(SelectQueryStruct qs) {
		//TODO: figure out how to use a qs with this
		return new MapHeaderDataRowIterator(this);
	}

	@Override
	public Set<String> getKeys() {
		return varMap.keySet();
	}
	
	/**
	 * Used to get all keys that point to the same object
	 * @param obj
	 */
	public Set<String> getAllAliasForObjectReference(Object obj) {
		Set<String> alias = new HashSet<String>();
		for(String key : varMap.keySet()) {
			NounMetadata noun = varMap.get(key);
			if(noun.getValue() == obj) {
				alias.add(key);
			}
		}
		return alias;
	}
	
	private String cleanVarName(String varName) {
		return varName.trim();
	}
}
