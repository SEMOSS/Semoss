package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class Stage extends Hashtable <String, Hashtable> {
	
	
	public static final String INPUTS = "INPUTS";
	public static final String DERIVED_INPUTS = "DERIVED_INPUTS";
	public static final String INPUT_STRING = "INPUT_STRING";
	public static final String OUTPUTS = "OUTPUTS";
	public static final String DEPENDS = "DEPENDS";
	public static final String CODE = "CODE";
	
	int stageNum = 0;
	// Vector of all the operations for purposes of sequence
	Vector <String> operationSequence = new Vector <String>();
	
	Vector <String> stageInputs = new Vector<String>();
	Vector <String> opVector = null;
	
	int numOps = 1;
	
	// should keep another hash of lowest dependency just in case things come in later
	Hashtable <String, Integer> lowestDependency = new Hashtable<String, Integer>();
	Hashtable <String, String> fromToDependency = new Hashtable <String, String>();
		
	public void addOperation(String operationName, Hashtable codeHash)
	{
		numOps++;
		// this code hash replaces the 
		put(operationName, codeHash);
		if(operationSequence.indexOf(operationName) < 0)
		{
			operationSequence.addElement(operationName);
			// it is dependent on itself
			lowestDependency.put(operationName, operationSequence.indexOf(operationName));
		}
	}
	
	public void synchronizeInput(String operationName)
	{
		Hashtable codeHash = getOperation(operationName);
		String inputString = " OPERATION COLUMNS  NEEDED ";
		Vector <String> inputs = (Vector<String>)codeHash.get(INPUTS);
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			inputString = inputString + "\t" + inputs.elementAt(inputIndex);
			addStageInput(inputs.elementAt(inputIndex));
		}
		// need to add the derived inputs here as well
		codeHash.put(INPUT_STRING, inputString);
		
		// also need to synchronize the dependencies
		Vector <String> depends = (Vector<String>)codeHash.get(DEPENDS);
		for(int depIndex = 0;depIndex < depends.size();depIndex++)
			setDependency(operationName, depends.elementAt(depIndex));
	}

	public void removeOperation(String operationName)
	{
		remove(operationName);
	}
	
	
	public Hashtable addOperation(String operationName)
	{
		if(containsKey(operationName))
			return getOperation(operationName);
		else
		{
			Hashtable codeHash = new Hashtable();
			codeHash.put(INPUTS, new Vector<String>());
			codeHash.put(DERIVED_INPUTS, new Vector<String>());
			codeHash.put(CODE, "// CODE Follows \n");
			codeHash.put(DEPENDS, new Vector<String>());
			addOperation(operationName, codeHash);
			return codeHash;
		}
	}
	
	public Hashtable getOperation(String operationName)
	{
		return get(operationName);
	}
	
	// to is what the from depends on
	// this has to be a recursive operation
	// need to find a way to do this
	// basically when I insert this.. I need to make sure I am ina sorted area
	// I feel like the existing tree takes care of it
	public void setDependency(String fromOperationName, String toOperationName)
	{
		int depNum = 0;
		if(lowestDependency.containsKey(toOperationName))
			depNum = lowestDependency.get(toOperationName);
		
		int fromIndex = operationSequence.indexOf(fromOperationName);
		int toIndex = operationSequence.indexOf(fromOperationName);
		
		// make sure all of them exist first in some way form or fashion
		if(toIndex >=0 && fromIndex >=0 && fromIndex >= toIndex) // this is where we need to adjust
		{
			operationSequence.remove(toOperationName);
			int addIndex = fromIndex - 1;
			if(addIndex < 0)
				addIndex = 0;
			operationSequence.insertElementAt(toOperationName, addIndex);
		}
		
		// both of them dont exist.. notsure this ever happens
		if(fromIndex < 0 && toIndex < 0)
		{
			addOperation(toOperationName);
			addOperation(fromOperationName);
		}
		
		if(fromIndex >= 0 && toIndex <0)
		{
			int addIndex = fromIndex - 1;
			if(addIndex < 0)
				addIndex = 0;
			operationSequence.insertElementAt(toOperationName, addIndex);
		}

		if(fromIndex < 0 && toIndex >= 0)
		{
			addOperation(fromOperationName);
		}
		
		// finally just update the dependency number
		if(depNum > fromIndex)
			lowestDependency.put(toOperationName, fromIndex);
		
	}
	
	public void addStageInput(String input)
	{
		if(stageInputs.indexOf(input) < 0)
			stageInputs.add(input);
	}
	
	public String getCode()
	{
		System.out.println("====================================");

		String retString = "// STAGE - " + stageNum + "\n{";
		
		// print all the inputs as a query
		String selectors = "";
		for(int inputIndex =0;inputIndex < stageInputs.size();inputIndex++)
		{
			if(selectors.length() == 0)
				selectors = stageInputs.elementAt(inputIndex);
			else
				selectors = selectors + "," + stageInputs.elementAt(inputIndex);
		}
		
		retString = retString + "\n";
		retString = retString + "// QUERY SELECTORS NEEDED for this STAGE " + selectors + "\n";
		retString = retString + selectors + "\n\n";
		
		for(int opIndex = 0;opIndex < operationSequence.size();opIndex++)
		{
			String thisOperation = operationSequence.elementAt(opIndex);
			if(containsKey(thisOperation))
			{
				Hashtable codeHash = (Hashtable)get(thisOperation);
				
				// get the inputs
				retString = retString +"\n //----------" + thisOperation + "--------------";
				retString = retString + "\n //" + codeHash.get(INPUT_STRING);
				// specify them here
				// get the code
				// add the code
				retString = retString + "\n" + codeHash.get(CODE);
				retString = retString +"\n //---------" + thisOperation + "---------------";				
			}
			retString = retString + "\n\n";
		}
		
		retString = retString + "\n }";
		System.out.println("====================================");

		return retString;
	}
	
	public Vector<String> getOperationsInStage()
	{
		if(opVector == null)
		{
			opVector = new Vector<String>();
			Enumeration<String> keys = keys();
			Vector <String> retVector = new Vector<String>();
			
			while(keys.hasMoreElements())
				retVector.addElement(keys.nextElement());
		}		
		return opVector;
	}

}
