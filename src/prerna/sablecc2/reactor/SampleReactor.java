package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import prerna.sablecc2.om.CodeBlock;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class SampleReactor extends AbstractReactor {

	String operationName = null;
	String signature = null;
	String curNoun = null;
	IReactor parentReactor = null;
	IReactor childReactor = null;
	NounStore store = null;
	IReactor.TYPE type = IReactor.TYPE.FLATMAP;
	IReactor.STATUS status = null;
	GenRowStruct curRow = null;
	
	String reactorName = "Sample";
	String [] asName = null;
	Vector <String> outputFields = null;
	Vector <String> outputTypes = null;
	
	Hashtable <String, Object> propStore = new Hashtable<String, Object>();
	
	PKSLPlanner planner = null;
	
	@Override
	public void In()
	{
		// set the stores and such
		System.out.println("Calling the in of" + operationName);
        curNoun("all");
		//if(parentReactor != null && parentReactor.getName().equalsIgnoreCase("EXPR"))
	}
	
	@Override
	public Object Out()
	{
		System.out.println("Calling the out of" + operationName);
		System.out.println("Calling the out of " + reactorName);
		// if the operation is fully sql-able etc
		// and it is a map operation.. in this place it should merge with the parent
		// there are 2 cases here
		// a. I can assimilate with the parent and let it continue to rip
		// b. I have to finish processing this before I give it off to parent
		
		// additionally.. if I do see a parent reactor.. I should add this as the input to the parent
		// so that we know what order to execute it
		
		updatePlan();
		
		if(this.type != IReactor.TYPE.REDUCE && this.store.isSQL())
		{
			// 2 more scenarios here
			// if parent reactor is not null
			// merge
			// if not execute it
			// if the whole thing is done through SQL, then just add the expression
			if(this.parentReactor != null)
			{
				mergeUp();
				return parentReactor;
			}
			// else assimilated with the other execute
/*			else
			{
				// execute it
			}
*/		
		}
		// the case below should not actually happen.. it should be done through the script chain
		else if(parentReactor == null)
		{
			// execute it
			//return execute();
		}
		else if(parentReactor != null) return parentReactor;
		// else all the merging has already happened
		return null;
	}
	
	// need a merge nounstore
	// this logic should sit inside the reactor not in state
	// this will be abstract eventually
	@Override
	public void mergeUp()
	{
		// looks at parent and then whatever this needs to do to merge
		// for instance when encountered in an expression
		// this should just make the expression and bind it in
		// not sure how the execution works yet
		System.out.println("Call for merging..");
		
		// this is actually fairly simple to do now
		// pick each one of the genrowstruct and merge it
		// first is overall noun rows
		// should we maintain the sequence here again ?
		// or let it be ?
		// may be keep the sequence
		// should the child come first or the parent ?
		// so many questions 
		// should be the parent
		// not going to keep count for now
		Enumeration <String> curReactorKeys = store.nounRow.keys();
		// if you want to keep order.. should be the work of the reactor
		while(curReactorKeys.hasMoreElements())
		{
			String thisNoun = curReactorKeys.nextElement();
			GenRowStruct output = store.nounRow.get(thisNoun);
			parentReactor.getNounStore().addNoun(thisNoun, output);
		}
		// For expression also add the fact that this will be 
		GenRowStruct exprStruct = new GenRowStruct();

		exprStruct.addColumn(signature);
		// p is for projection
		parentReactor.getNounStore().addNoun(NounStore.projector, exprStruct);
	}
	
	// execute it
	// once again this would be abstract
	@Override
	public Object execute()
	{
		System.out.println("Execute the method.. " + signature);
		System.out.println("Printing NOUN Store so far.. " + store);
		return null;
	}
	
	@Override
	public void updatePlan()
	{
		// add the inputs from the store as well as this operation
		// first is all the inputs
		getType();
		Enumeration <String> keys = store.nounRow.keys();
		
		
		String reactorOutput = reactorName;
		
		while(keys.hasMoreElements())
		{
			String singleKey = keys.nextElement();
			GenRowStruct struct = store.nounRow.get(singleKey);
			Vector <String> inputs = struct.getAllColumns();
			Vector <Object> filters = struct.getColumnsOfType(GenRowStruct.COLUMN_TYPE.FILTER);
			Vector <Object> joins = struct.getColumnsOfType(GenRowStruct.COLUMN_TYPE.JOIN);
			
			// need a better way to do it
			if(asName == null)
				reactorOutput = reactorOutput + "_" + struct.getColumns();

			// find if code exists
			if(!propStore.containsKey("CODE"))
			{
				if(inputs.size() > 0)
					planner.addInputs(signature, inputs, type);
				if(filters != null && filters.size() > 0)
					planner.addProperty(signature, "FILTERS", filters);
				if(joins != null && joins.size() > 0)
					planner.addProperty(signature, "JOINS", joins);
			}
			else
			{
				// this is a code block
				String code = (String)propStore.get("CODE");
				
				CodeBlock.LANG thisLang = CodeBlock.LANG.JAVA; // default
				// need a if loop to convert language to java
				if(propStore.containsKey("LANGUAGE"))
				{
					String language = (String)propStore.get("LANGUAGE");
					if(language.toLowerCase().contains("python"))
						thisLang = CodeBlock.LANG.PYTHON;
					if(language.toLowerCase().contains("r")) // good luck if you want prolog it will still be R :)
						thisLang = CodeBlock.LANG.R;
				}
				CodeBlock thisBlock = new CodeBlock();
				thisBlock.setLanguage(CodeBlock.LANG.JAVA);
				thisBlock.addCode(code);
				
				// add it as a code block
				planner.addInputs(signature, thisBlock, inputs, IReactor.TYPE.MAP);
				if(filters != null && filters.size() > 0)
					planner.addProperty(signature, "FILTERS", filters);
				if(joins != null && joins.size() > 0)
					planner.addProperty(signature, "JOINS", joins);
			}
			
			// also need to take care of filters here
			// as well as joins
			// the more I think.. there is no abstraction it is just a spout
			// GOD  !!
			// may be we should keep query struct at reactor level
			// may be not
		}
		
		// give it a variable name
		if(asName == null)
			asName = new String[]{reactorOutput};
			
		// I also need to accomodate when this happens in a chain
		// this has to happen directly through the as reactor
		// couple of things here
		// the output fields have to be specified
		// i.e. the reactor should update the output fields
		// if not, it is assumed that it is same as input field ? possibly ?
		// it can never be the same as input field
		// not sure what is the point then

		// it is very much dependent on the operation
		
		// if not the as should take care of it ?
		if(outputFields == null)
		{
			outputFields = new Vector<String>();
			outputFields.add(asName[0]); // for this reactor there is always only 1
		}
		// second is all the outputs
		// need to think about this a bit more
		// should I add it to the parent or the parent will add it by itself ?
		planner.addOutputs(signature, outputFields, type);
		// also add properties to these outputfields
		//planner.addProperty(opName, propertyName, value);
	}

}