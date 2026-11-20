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
package prerna.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;

import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.GenRowStruct;

public abstract class AbstractLambdaTaskReactor extends TaskBuilderReactor {

	static final String IMPORTS_KEY = "imports";

	/**
	 * Return the code block for the lambda function
	 * 
	 * @return
	 */
	protected String getCode() {
		String code = (String) this.propStore.get("CODE");
		return code;
	}

	/**
	 * Get imports to add as part of the class
	 * 
	 * @return
	 */
	protected List<String> getImports() {
		List<String> imports = new ArrayList<String>();
		GenRowStruct importGrs = this.store.getGenRowStruct(IMPORTS_KEY);
		if (importGrs != null && !importGrs.isEmpty()) {
			int size = importGrs.size();
			for (int i = 0; i < size; i++) {
				imports.add(importGrs.get(i).toString());
			}
		}
		return imports;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals("CODE")) {
			return "The code block for the lambda function";
		} else if (key.equals(IMPORTS_KEY)) {
			return "The imports to add as part of the class";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
