package prerna.reactor.task.modifiers;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.ClassMaker;
import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class FilterLambdaReactor extends AbstractLambdaTaskReactor {

	/**
	 * Abstract lambda class is responsible for getting
	 * data from the noun store / prop store
	 */
	
	public FilterLambdaReactor() {
		this.keysToGet = new String[]{"CODE", IMPORTS_KEY};
	}
	
	@Override
	protected void buildTask() {
		String code = getCode();
		List<String> imports = getImports();
		
		List<Map<String, Object>> headerInfo = this.task.getHeaderInfo();
		List<String> varNames = new Vector<String>();
		List<String> types = new Vector<String>();
		int size = headerInfo.size();
		for(int i = 0; i < size; i++) {
			Map<String, Object> headerMap = headerInfo.get(i);
			varNames.add(headerMap.get("alias").toString());
			types.add(headerMap.get("type").toString().toUpperCase());
		}
		
		// class maker will help us compile our new lambda function 
		ClassMaker myClass = new ClassMaker("prerna.sablecc2.reactor.task.modifiers", Utility.getRandomString(12));
		// extends the generic java task map operation
		String baseClass = "prerna.sablecc2.reactor.task.modifiers.FilterTaskIterator";
		myClass.addSuper(baseClass);
		// add all the imports
		for(int i = 0; i > imports.size(); i++) {
			String importPackage = imports.get(i).trim();
			if(importPackage.endsWith(";")) {
				importPackage = importPackage.substring(0, importPackage.length()-1);
			}
			myClass.addImport(importPackage);
		}
		
		// now, we will create the method for the iterator
		StringBuffer method = new StringBuffer();
		method.append("public boolean isValidRow(IHeadersDataRow random_dont_conflict_row) { ");
		method.append("Object[] random_dont_conflict_values = random_dont_conflict_row.getValues(); ");
		method.append("String[] headers = random_dont_conflict_row.getHeaders(); ");
		for(int i = 0; i < size; i++) {
			String varName = varNames.get(i);
			String type = types.get(i);
			if(type.equalsIgnoreCase("STRING")) {
				method.append("String ").append(varName).append(" = random_dont_conflict_values[").append(i).append("].toString(); ");
			} else if(type.equalsIgnoreCase("NUMBER")) {
				method.append("Double ").append(varName).append(" = (Double) random_dont_conflict_values[").append(i).append("]; ");
			}
		}
		method.append(code);
		method.append("}");
		myClass.addMethod(method.toString());
		
		// now we will create a new lambda and feed into it the 
		// previous iterator
		try {
			AbstractTaskOperation newTask = (AbstractTaskOperation) myClass.toClass().newInstance();
			newTask.setInnerTask(this.task);
			// and then the reference to the new task
			this.task = newTask;
			// also add this to the store!!!
			this.insight.getTaskStore().addTask(this.task);
			return;
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		
		// if we get here
		// there was an error
		throw new IllegalArgumentException("Error with creating generic lambda!");
	}
}

abstract class FilterTaskIterator extends AbstractTaskOperation {

	private IHeadersDataRow nextRow = null;
	
	public FilterTaskIterator() {
		
	}
	
	public FilterTaskIterator(ITask innerTask) {
		super(innerTask);
	}

	// this is the method to override
	abstract boolean isValidRow(IHeadersDataRow random_dont_conflict_row);
	
	@Override
	public boolean hasNext() {
		// try to get the next row!
		if(this.nextRow == null) {
			while(this.innerTask.hasNext() && this.nextRow == null) {
				IHeadersDataRow testRow = this.innerTask.next();
				boolean isValid = isValidRow(testRow);
				if(isValid) {
					this.nextRow = testRow;
				}
			}
		}
		
		// if it is still null, return false
		// else, return true
		if(this.nextRow != null) {
			return true;
		}
		
		return false;
	}
	
	@Override
	public IHeadersDataRow next() {
		// store the valid row we have cached
		IHeadersDataRow nextValidRow = this.nextRow;
		// null out the reference so we get the
		this.nextRow = null;
		return nextValidRow;
	}
}
