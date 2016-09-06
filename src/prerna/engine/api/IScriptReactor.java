package prerna.engine.api;

import prerna.sablecc.meta.IPkqlMetadata;

public interface IScriptReactor extends IApi {
	

	// adds a pattern to be replaced
	public void addReplacer(String pattern, Object value);
	
	// gets the values for a given child to be synchronized
	// useful whenyou need more details
	public String [] getValues2Sync(String childName);
	
	// remove a given replacer
	// when I add up stuff if I can see the value
	// then I want to replace it with the higher level piece
	public void removeReplacer(String pattern);
		
	// get me a particular value
	public Object getValue(String key);
	
	// puts a particular value 
	public void put(String key, Object value);

//	public String explain();

	// I am not sure if I need anything else ?
	// I will know once I implement the expr reactor
	
	public IPkqlMetadata getPkqlMetadata();
	
	// MAHER TEST CODE
	// IGNORE
//	// store the value for a specific pkql component
//	void addComponentValue(String componenet, Object value);
//	
//	// store the value to replace a specific pkql portion with a value
//	// will be used to replace addReplacer logic
//	void addExpressionToValue(String componenet, Object value);
	
}
