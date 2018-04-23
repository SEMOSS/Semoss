package prerna.sablecc2.reactor.task.transformation.map;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.ClassMaker;
import prerna.util.Utility;

public class GenericMapTransformation extends AbstractMapTransformation {

	private static final String CLASS_NAME = BaseMapTransformation.class.getName();
	private IMapTransformation generatedClass;

	public GenericMapTransformation() {
		
	}
	
	public void init(String code, List<String> imports) throws InstantiationException, IllegalAccessException {
		// class maker will help us compile our new lambda function 
		ClassMaker myClass = new ClassMaker("prerna.sablecc2.reactor.task.transformation.map", "c" + Utility.getRandomString(12));
		// extends the map transformation interface
		myClass.addSuper(CLASS_NAME);
		// add all the imports
		for(int i = 0; i > imports.size(); i++) {
			String importPackage = imports.get(i).trim();
			if(importPackage.endsWith(";")) {
				importPackage = importPackage.substring(0, importPackage.length()-1);
			}
			myClass.addImport(importPackage);
		}
		
		// now, we will create the method for the transformation based on the input code
		StringBuffer method = new StringBuffer();
		method.append("public IHeadersDataRow process(IHeadersDataRow row) { ");
		method.append(code);
		method.append("}");
		myClass.addMethod(method.toString());
		
		// now generate the new transformation class
		// which will override the process method
		this.generatedClass = (BaseMapTransformation) myClass.toClass().newInstance();
	}
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		return generatedClass.process(row);
	}

	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		// do nothing
		// uses the above int function
	}

}

/**
 * I just need a base class with a constructor that is a IMapTransformation
 * Cannot use the interface or will get an error
 *
 */
class BaseMapTransformation extends AbstractMapTransformation {

	public BaseMapTransformation() {
		
	}
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		return row;
	}

	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		// do nothing
	}

}