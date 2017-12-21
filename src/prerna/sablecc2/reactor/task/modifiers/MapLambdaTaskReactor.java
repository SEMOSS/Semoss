package prerna.sablecc2.reactor.task.modifiers;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.task.AbstractTaskOperation;
import prerna.sablecc2.reactor.ClassMaker;

public class MapLambdaTaskReactor extends AbstractLambdaTaskReactor {

	/**
	 * Abstract lambda class is responsible for getting
	 * data from the noun store / prop store
	 */
	
	public MapLambdaTaskReactor() {
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
		ClassMaker myClass = new ClassMaker();
		// extends the generic java task map operation
		String baseClass = "prerna.sablecc2.om.task.AbstractTaskOperation";
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
		method.append("public IHeadersDataRow next() { ");
		method.append("IHeadersDataRow random_dont_conflict_row = this.innerTask.next(); ");
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
