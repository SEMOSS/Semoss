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
package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;

@Deprecated
public class UpdateQueryStruct extends AbstractQueryStruct {

	private List<Object> values = new ArrayList<>();

	/**
	 * Default constructor
	 */
	@Deprecated
	public UpdateQueryStruct() {

	}

	//////////////////////////////////////////// SELECTORS
	//////////////////////////////////////////// /////////////////////////////////////////////////

	@Deprecated
	@Override
	public void addSelector(IQuerySelector selector) {
		if (selector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
			throw new IllegalArgumentException("Can only add column selector for update queries");
		}
		this.selectors.add(selector);
	}

	//////////////////////////////////////////// VALUES
	//////////////////////////////////////////// ////////////////////////////////////////////////////

	@Deprecated
	public List<Object> getValues() {
		return this.values;
	}

	@Deprecated
	public void setValues(List<Object> values) {
		this.values = values;
	}

	/**
	 * 
	 * @param incomingQS This method is responsible for merging "incomingQS's" data
	 *                   with THIS querystruct
	 */
	@Deprecated
	@Override
	public void merge(AbstractQueryStruct incomingQS) {
		super.merge(incomingQS);
		if (incomingQS instanceof UpdateQueryStruct) {
			UpdateQueryStruct updateQS = (UpdateQueryStruct) incomingQS;
			mergeValues(updateQS.values);
		}
	}

	private void mergeValues(List<Object> values) {
		for (Object val : values) {
			if (!this.values.contains(val)) {
				this.values.add(val);
			}
		}
	}

	/**
	 * Gets a new QS with the base information moved over This is basically the qs
	 * type + enginename + csv/excel properties Note csv/excel qs overrides this
	 * method
	 * 
	 * @return
	 */
	@Deprecated
	public UpdateQueryStruct getNewBaseQueryStruct() {
		UpdateQueryStruct newQs = new UpdateQueryStruct();
		newQs.setQsType(this.qsType);
		if (this.qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE) {
			newQs.setEngineId(this.engineId);
			newQs.setEngine(this.engine);
		} else if (this.qsType == AbstractQueryStruct.QUERY_STRUCT_TYPE.FRAME) {
			newQs.setFrame(this.frame);
		}
		newQs.setValues(this.values);
		return newQs;
	}

}
