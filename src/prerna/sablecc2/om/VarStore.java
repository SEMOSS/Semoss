package prerna.sablecc2.om;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.MapHeaderDataRowIterator;
import prerna.query.parsers.ParamStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class VarStore implements InMemStore<String, NounMetadata> {

	public static final String PARAM_STRUCT_PREFIX = "$PARAM_STRUCT_";
	public static final String PARAM_STRUCT_PD_PREFIX  = "$PD_PARAM_STRUCT_";
	
	// the main object where all the nouns are stored
	private Map<String, NounMetadata> varMap;
	
	// all the frames that are generated
	private Set<ITableDataFrame> allCreatedFrames;
	
	// for quick searching
	// storing the varNames for all frames
	private List<String> frameKeys;
	
	// for quick searching
	// storing the varnames for all insight parameters
	private List<String> insightParametersKeys;
	
	// storing the varnames for all pre-defined insight parameters
	private List<String> preDefinedParametersKeys;
	
	// store dynamic variables
	private List<String> dynamicVars;
	
	public VarStore() {
		varMap = new HashMap<>();
		frameKeys = new ArrayList<>();
		insightParametersKeys = new ArrayList<>();
		preDefinedParametersKeys = new ArrayList<>();
		allCreatedFrames = new HashSet<>();
		dynamicVars = new ArrayList<>();
	}
	
	@Override
	public synchronized void put(String varName, NounMetadata variable) {
		varName = cleanVarName(varName);
		if(variable.getNounType() == PixelDataType.COLUMN) {
			if(varName.equals(variable.getValue().toString())) {
				return;
			}
		}
		varMap.put(varName, variable);
		// keep quick reference to frames
		if(variable.getNounType() == PixelDataType.FRAME) {
			if(!frameKeys.contains(varName)) {
				frameKeys.add(varName);
			}
			// we will store a hash of all the frames
			// just in case a reference gets overwritten
			// so we can clear them out
			allCreatedFrames.add((ITableDataFrame) variable.getValue());
		} else if(variable.getNounType() == PixelDataType.PARAM_STRUCT) {
			if(!insightParametersKeys.contains(varName)) {
				insightParametersKeys.add(varName);
			}
		} else if(variable.getNounType() == PixelDataType.PREAPPLIED_PARAM_STRUCT) {
			if(!preDefinedParametersKeys.contains(varName)) {
				preDefinedParametersKeys.add(varName);
			}
		} else if(variable.getNounType() == PixelDataType.VARIABLE) {
			if(!dynamicVars.contains(varName)) {
				dynamicVars.add(varName);
			}
		}
	}
	
	public synchronized void putAll(VarStore otherStore) {
		varMap.putAll(otherStore.varMap);
		frameKeys.addAll(otherStore.frameKeys);
		insightParametersKeys.addAll(otherStore.insightParametersKeys);
		preDefinedParametersKeys.addAll(otherStore.preDefinedParametersKeys);
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
	public synchronized NounMetadata remove(String varName) {
		// also try to remove from frameSet if its a frame
		this.frameKeys.remove(varName);
		this.insightParametersKeys.remove(varName);
		this.preDefinedParametersKeys.remove(varName);
		this.dynamicVars.remove(varName);
		return varMap.remove(varName);
	}
	
	/**
	 * Remove all keys 
	 * @param keys
	 */
	public synchronized void removeAll(Collection<String> keys) {
		// also try to remove from frameSet if its a frame
		this.frameKeys.removeAll(keys);
		this.insightParametersKeys.removeAll(keys);
		this.preDefinedParametersKeys.removeAll(keys);
		this.dynamicVars.removeAll(keys);
		this.varMap.keySet().removeAll(keys);
	}
	
	@Override
	public synchronized void clear() {
		this.frameKeys.clear();
		this.insightParametersKeys.clear();
		this.preDefinedParametersKeys.clear();
		this.dynamicVars.clear();
		this.varMap.clear();
	}
	
	@Override
	public IRawSelectWrapper getIterator() {
		return new MapHeaderDataRowIterator(this);
	}

	@Override
	public IRawSelectWrapper getIterator(SelectQueryStruct qs) {
		//TODO: figure out how to use a qs with this
		return new MapHeaderDataRowIterator(this);
	}

	@Override
	public Set<String> getKeys() {
		return varMap.keySet();
	}
	
	public List<String> getFrameKeysCopy() {
		return new ArrayList<>(frameKeys);
	}
	
	/**
	 * This method returns all the created frames
	 * This is not a copy - so beware of concurrent modifications
	 * This is only used right now when we are clearing the insight
	 * and want to release all the frames used
	 * @return
	 */
	public Set<ITableDataFrame> getAllCreatedFrames() {
		return allCreatedFrames;
	}
	
	/**
	 * Get all the references for a specific frame
	 * @param frame
	 * @return
	 */
	public Set<String> findAllVarReferencesForFrame(ITableDataFrame frame) {
		Set<String> referenceSet = new HashSet<>();
		for(String frameKey : frameKeys) {
			NounMetadata possibleFrameVar = this.varMap.get(frameKey);
			if(possibleFrameVar.getValue() == frame) {
				referenceSet.add(frameKey);
			}
		}
		return referenceSet;
	}
	
	/**
	 * Get a set of all the frames
	 * @return
	 */
	public Set<ITableDataFrame> getFrames() {
		Set<ITableDataFrame> frames = new HashSet<>();
		for(String frameKey : frameKeys) {
			NounMetadata frameNoun = this.varMap.get(frameKey);
			ITableDataFrame frame = (ITableDataFrame) frameNoun.getValue();
			frames.add(frame);
		}
		
		return frames;
	}
	
	public List<String> getInsightParameterKeys() {
		return Collections.unmodifiableList(insightParametersKeys);
	}
	
	public List<String> getPreDefinedParametersKeys() {
		return Collections.unmodifiableList(preDefinedParametersKeys);
	}
	
	public List<String> getDynamicVarKeys() {
		return Collections.unmodifiableList(dynamicVars);
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
	
	/**
	 * Pull the insight parameters
	 * @return
	 */
	public Map<String, NounMetadata> pullParameters() {
		Map<String, NounMetadata> retMap = new LinkedHashMap<>();
		for(String paramKey : this.insightParametersKeys) {
			retMap.put(paramKey, this.varMap.get(paramKey));
		}
		
		return retMap;
	}
	
	/**
	 * Pull the preApplied parameters
	 * @return
	 */
	public Map<String, NounMetadata> pullPreAppliedParameters() {
		Map<String, NounMetadata> retMap = new LinkedHashMap<>();
		for(String paramKey : this.preDefinedParametersKeys) {
			retMap.put(paramKey, this.varMap.get(paramKey));
		}
		
		return retMap;
	}
	
	/**
	 * Pull param structs that are saved in the insight
	 * @return
	 */
	public List<ParamStruct> pullParamStructs() {
		List<ParamStruct> params = new Vector<>();
		for(String paramKey : insightParametersKeys) {
			params.add((ParamStruct) varMap.get(paramKey).getValue());
		}
		
		return params;
	}
	
	// need to find why the frames is not a hashmap and/or 2 list and instead a set and list
	public ITableDataFrame getFrame(String name)
	{
		ITableDataFrame retFrame = null;
		Iterator <ITableDataFrame> allFrames = allCreatedFrames.iterator();
		while(allFrames.hasNext())
		{
			ITableDataFrame thisFrame = allFrames.next();
			if(thisFrame.getName().contentEquals(name))
			{
				retFrame = thisFrame;
				break;
			}
		}
		return retFrame;
	}

}
