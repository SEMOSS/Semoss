package prerna.reactor;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.Filter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Utility;

public class Stage extends Hashtable <String, Hashtable> {
	
	// stage can also be broken into more specific bolts but for now
	
	public static final String INPUTS = "INPUTS";
	public static final String DERIVED_INPUTS = "DERIVED_INPUTS";
	public static final String INPUT_STRING = "INPUT_STRING";
	public static final String OUTPUTS = "OUTPUTS";
	public static final String DEPENDS = "DEPENDS";
	public static final String CODE = "CODE";
	public static final String INPUT_TYPE = "INPUT_TYPE";
	
	StringBuffer queryStructString = new StringBuffer("public void makeQuery() \n{ \n qs = new QueryStruct(); "
			+ "\n String tableName = \"TBD\";\n"
			+ "if(this.frame != null)\n tableName = this.frame.getTableName();"
			+ "\n");
	int stageNum = 0;
	// Vector of all the operations for purposes of sequence
	Vector <String> operationSequence = new Vector <String>();
	
	Vector <String> stageInputs = new Vector<String>();
	Vector <String> opVector = null;
	
	int numOps = 1;
	Lambda runner = null;
	
	Hashtable <String, Object> stageStore = new Hashtable<String, Object>();
	
	// should keep another hash of lowest dependency just in case things come in later
	Hashtable <String, Integer> lowestDependency = new Hashtable<String, Integer>();
	Hashtable <String, String> fromToDependency = new Hashtable <String, String>();
	
	Vector <Object> filters = new Vector<Object>();
	Vector <Object> joins = new Vector<Object>();
			
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
		Hashtable <String, String> inputTypes = (Hashtable <String, String>)codeHash.get(Stage.INPUT_TYPE);
		String inputString = "{";
		Vector <String> inputs = (Vector<String>)codeHash.get("INPUT_ORDER");
		Vector <String> asInputs = (Vector<String>)codeHash.get("ALIAS");
		
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			if(inputIndex == 0)
				inputString = inputString + "\"" + inputs.elementAt(inputIndex) + "\"";
			else
				inputString = inputString + ", \"" + inputs.elementAt(inputIndex) + "\"";
			
			// need to do a quick check here to say
			// is this a query on input types before adding it
			// if it is not a query.. there is no point trying to query it
			if(inputTypes.containsKey(inputs.elementAt(inputIndex)) && inputTypes.get(inputs.elementAt(inputIndex)).equalsIgnoreCase("QUERY"))
				addStageInput(inputs.elementAt(inputIndex), asInputs.elementAt(inputIndex));
		}
		
		// take care of filters here
		Vector <Object> filters = (Vector<Object>)codeHash.get("FILTERS");
		this.filters.addAll(filters);
		
		/*for(int filterIndex = 0;filters != null && filterIndex < filters.size();filterIndex++)
		{
			Object thisFilter = filters.elementAt(filterIndex);
			if(thisFilter instanceof Join)
				System.out.println("Instance of join.. ");
			else
				System.out.println("Weird.. ");;
			
			addStageFilter((Filter)thisFilter);
		}*/

		Vector <Object> joins = (Vector<Object>)codeHash.get("JOINS");
		this.joins.addAll(joins);
		
		/*for(int joinIndex = 0;joins != null && joinIndex < joins.size();joinIndex++)
		{
			Object thisJoin = joins.elementAt(joinIndex);
			if(thisJoin instanceof Join)
				System.out.println("Instance of join.. ");
			else
				System.out.println("Weird.. ");;
			
			addStageRelation((Join)thisJoin);
		}*/
		
		// get the frame from the codehash and set it up
		// frame over writes i.e. if you have one operation later setting the frame
		// that will take precedence
		// adds the property
		// and sets the frame
		if(codeHash.containsKey("FRAME"))
			stageStore.put("FRAME", codeHash.get("FRAME"));
		
		// TODO: need to do the property.. we will do this later
		// not checking for namespace collision
		if(codeHash.containsKey("STORE"))
		{
			Hashtable <String, Object> propStore = (Hashtable <String, Object>)codeHash.get("STORE");
			Enumeration <String> keys = propStore.keys();
			while(keys.hasMoreElements())
			{
				String key = keys.nextElement();
				stageStore.put(key, propStore.get(key));
			}
		}
		/*
		// need to add the derived inputs here as well
		inputs = (Vector<String>)codeHash.get(this.DERIVED_INPUTS);
		for(int inputIndex = 0;inputIndex < inputs.size();inputIndex++)
		{
			if(inputIndex == 0)
				inputString = "\"" + inputs.elementAt(inputIndex) + "\"";
			else
				inputString = inputString + ", \"" + inputs.elementAt(inputIndex) + "\"";
			addStageInput(inputs.elementAt(inputIndex));
		}
		*/
		inputString = inputString + "};";
		if(inputString.equalsIgnoreCase("{};"))
			inputString = "";

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
			String [] codeFollows = new String[2];
			codeFollows[0] = "";
			codeHash.put(CODE, codeFollows);
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
	
	public void addStageInput(String input, String asInput)
	{
		if(stageInputs.indexOf(input) < 0)
		{
			stageInputs.add(input);
			String [] column = input.split("__");
			if(column.length == 1)
				queryStructString.append("qs.addSelector(tableName , \"" + input +"\");\n" );
			else
				queryStructString.append("qs.addSelector(\"" + column[0] + "\",  \""+ column[1] +"\");\n" );
		}
	}
	
	// adds all the filters
	// need to ask maher.. if I need to do the same for the filters i.e. add tablename + column name
	public void addStageFilter(Filter filter)
	{
		// need to do this later
		// the problem is we need to convert this into a number
		// and convert it back
		// for now we will assume the type is the same
		// that is they are all strings or they are all numbers
		GenRowStruct values = filter.getRComparison();
		if(values.size() > 0)
		{
			// predict what the type is
			PixelDataType type = values.vector.get(1).getNounType();
			String pad = "";
			StringBuffer filterVectorString = new StringBuffer();
			if(type == PixelDataType.CONST_STRING)
			{
				pad = "\"";
				filterVectorString.append("strVector = new Vector();\n");
				// now make this into a vector
				for(int valIndex = 1;valIndex < values.size();valIndex++)
					filterVectorString.append("strVector.addElement(" + pad + values.get(valIndex) + pad + ");\n");
				filterVectorString.append("qs.addFilter(\"" + filter.getLComparison().get(0).toString() + "\", \"" + filter.getComparator() + "\" ,strVector);\n");
			}
			else
			{
				filterVectorString.append("decVector = new Vector();\n");
				for(int valIndex = 1;valIndex < values.size();valIndex++)
				{
					Object doubVal = values.get(valIndex);
					filterVectorString.append("decVector.addElement(new Double(" + values.get(valIndex) +"));\n");
				}
				filterVectorString.append("qs.addFilter(\"" + filter.getLComparison().get(0).toString() + "\", \"" + filter.getComparator() + "\",decVector);\n");
			}
			System.out.println("FILTER..... " + filterVectorString);
			queryStructString.append(filterVectorString);
		}		
	}
	
	// adds the relationship joins
	public void addStageRelation(Join filter)
	{
		// need to do this later
		// the problem is we need to convert this into a number
		// and convert it back
		// for now we will assume the type is the same
		// that is they are all strings or they are all numbers
			// predict what the type is
		String 	pad = "\"";
		StringBuffer relationString = new StringBuffer();
		relationString.append("strVector = new Vector();\n");
		relationString.append("qs.addRelation(\"" + filter.getLColumn() + "\", \"" +filter.getRColumn() + "\" ,\"" + filter.getJoinType() + "\");\n");
		System.out.println("RELATION..... " + relationString);
		queryStructString.append(relationString);
	}
	
	// really simple
	// if the table is not there adds the table frame to it
	// TBD after discussion with maher / rishi
	public String qualifyColumn(String column)
	{
		return null;
	}

	public String getCode()
	{
		System.out.println("STAGE " + stageNum + "====================================");
		
		ClassMaker thisClass = new ClassMaker(); // go with dynamic for now
		thisClass.addSuper("prerna.reactor.Lambda");
		
		// finish the query block
		// this makes the query
		queryStructString.append("\n");
		queryStructString.append("if(frame != null) \n thisIterator = frame.query(this.qs);\n else \n\t System.out.println(\"Frame is null\");\n}\n");
		
		
		// this creates all the inputs
		StringBuffer declareBlock = new StringBuffer("public void addInputs() \n{\n");

		// this is the main function for executing the code
		StringBuffer executeBlock = new StringBuffer("public IHeadersDataRow executeCode(IHeadersDataRow curRow) \n{\n IReactor thisReactor = null;");

		StringBuffer queryBlock = new StringBuffer("public void iterateAll() \n{\n");
		queryBlock = queryBlock.append("while(thisIterator.hasNext()) \n{");
		queryBlock = queryBlock.append("executeFunction(thisIterator.next()); \n");
		queryBlock = queryBlock.append("};");
		
				
		// this is all the other functions ?
		StringBuffer endBlock = new StringBuffer("");

		StringBuffer retString = new StringBuffer("");
		
		//no longer valid
		/*
		// print all the inputs as a query
		String selectors = "";
		// Fill the query Struct
		
		
		for(int inputIndex =0;inputIndex < stageInputs.size();inputIndex++)
		{
			if(selectors.length() == 0)
				selectors = stageInputs.elementAt(inputIndex);
			else
				selectors = selectors + "," + stageInputs.elementAt(inputIndex);
		}
		
		retString = retString.append("\n");
		retString = retString.append("// QUERY SELECTORS NEEDED for this STAGE " + selectors + "\n");
		retString = retString.append(selectors);
		*/
		
		for(int opIndex = 0;opIndex < operationSequence.size();opIndex++)
		{
			String thisOperation = operationSequence.elementAt(opIndex);
			if(containsKey(thisOperation))
			{
				Hashtable codeHash = (Hashtable)get(thisOperation);
				
				// get the inputs
				retString = retString.append("\n //----------" + thisOperation + "--------------");
				// compose a string with all the inputs to be added later
				retString = retString.append("\n //" + codeHash.get(INPUT_STRING));

				// add the declarations into declarations block
				// inputStore.put
				String [] code = (String[])codeHash.get(CODE);
				if(code[0] != null && code[0].length() != 0)
				{
					String inputStringName = Utility.getRandomString(8) + "Inputs";
					String inputString = (String)codeHash.get(INPUT_STRING);
					declareBlock = declareBlock.append("\nString [] " + inputStringName + " = " + inputString + ";");
					declareBlock = declareBlock.append("\ninputStore.put( \"" + inputStringName + "\", " + inputStringName +");");
				
					// I need to do these pieces on for every
					String rowObjectName = Utility.getRandomString(8) + "RowObject"; 
					retString = retString.append("\nObject [] " + rowObjectName + " = " + "getRow(\"" + inputStringName + "\", curRow);");
					executeBlock.append("\nObject [] " + rowObjectName + " = " + "getRow(\"" + inputStringName + "\", curRow);");
					// specify them here
					// get the code
					// add the code
					retString = retString.append("\nrun" + code[0] + "(" + rowObjectName + ");");
					executeBlock.append("\nrun" + code[0] + "(" + rowObjectName + ");");
					// add all the end blocks
					thisClass.addMethod(code[1]);
					endBlock = endBlock.append("\n" + code[1]);
				}
				if(codeHash.containsKey("REACTOR"))
				{
					executeBlock.append("thisReactor = (IReactor)store.get(" + codeHash.get("SIGNATURE") + ");\n");
					if(codeHash.get("OP_TYPE") == IReactor.TYPE.MAP)
					{
						executeBlock.append("curRow = thisReactor.execute(curRow);");
					}
					else // this is a reduce
					{
						// need to do reduce here
						executeBlock.append("curRow = thisReactor.reduce(thisIterator);");
					}
				}
				retString = retString.append("\n //---------" + thisOperation + "---------------");				
			}
			retString = retString.append("\n\n");
		}
		executeBlock.append("return curRow;");
		executeBlock.append("}");
		thisClass.addMethod(executeBlock +"");
		declareBlock.append("}");
		retString = retString.append("\n } ");
		System.out.println(declareBlock);
		thisClass.addMethod(declareBlock.toString());
		retString = retString.insert(0,declareBlock + "\n"); // insert all tthe declarations upfront
		System.out.println("Query Struct now.. \n\n" + queryStructString);
		//thisClass.addMethod(queryStructString.toString()); <-- I dont need this anymore
		retString = retString.insert(0, queryStructString + "\n");
		retString = retString.append("\n // Other code functions follow ");
		retString = retString.append("\n" + endBlock);
		retString.append("====================================\n");

		try {
			runner = (Lambda)(thisClass.toClass().newInstance());
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		runner.test();
		//thisClass.writeClass("C:\\Users\\pkapaleeswaran\\workspacej3\\SemossDev\\Codagen\\Stage" + stageNum + ".java");
		return retString.toString();
		
	}
	
	// need to introduce a pre-process call
	// where if I have the frame in the operation
	// I need to set it etc. 
	public void preProcessStage()
	{
		if(stageStore.containsKey("FRAME"))
		{
			runner.setFrame((ITableDataFrame)stageStore.get("FRAME"));
			stageStore.remove("FRAME");
		}
		// this will synchronize all the reactors
		runner.addStore(stageStore);
	}
	
	public void processStage()
	{
		runner.makeQuery();
		runner.addInputs();
		runner.execute();
	}
	
	public Hashtable<String, Object> postProcessStage()
	{
		// need to get the data back so we can reset it
		return stageStore;
	}
	
	public void addStore(Hashtable <String, Object> newStore)
	{
		Enumeration <String> keys = newStore.keys();
		while(keys.hasMoreElements())
		{
			String key = keys.nextElement();
			this.stageStore.put(key, newStore.get(key));
		}
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
