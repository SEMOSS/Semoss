package prerna.sablecc2.reactor;

import java.util.Enumeration;
import java.util.Vector;

import prerna.sablecc2.om.CodeBlock;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.GenRowStruct.COLUMN_TYPE;

public class ImportDataReactor extends AbstractReactor {


	
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
			Vector <Object> db = struct.getColumnsOfType(COLUMN_TYPE.CONST_STRING);
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
				if(db != null && db.size() > 0) {
					planner.addProperty(signature, "ENGINE", db.get(0));
				}
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

	@Override
	public void In() {
		
	}

	@Override
	public Object Out() {
		return null;
	}

	@Override
	public void mergeUp() {
		
	}

	@Override
	public Object execute() {
		return null;
	}
}
