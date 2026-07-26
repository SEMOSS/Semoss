/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.task.lambda.map;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.ClassMaker;
import prerna.util.Utility;

public class GenericMapLambda extends AbstractMapLambda {

	private static final String CLASS_NAME = BaseMapLambda.class.getName();
	private IMapLambda generatedClass;

	public GenericMapLambda() {
		
	}
	
	public void init(String code, List<String> imports) throws InstantiationException, IllegalAccessException {
		// class maker will help us compile our new lambda function 
		ClassMaker myClass = new ClassMaker(AbstractMapLambda.class.getPackage().getName(), "c" + Utility.getRandomString(12));
		// extends the map transformation interface
		myClass.addSuper(CLASS_NAME);
		// add all the imports
		for(int i = 0; i < imports.size(); i++) {
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
		this.generatedClass = (BaseMapLambda) myClass.toClass().newInstance();
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
class BaseMapLambda extends AbstractMapLambda {

	public BaseMapLambda() {
		
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