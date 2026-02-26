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
package prerna.junit.reactors.query;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Test;

import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class PandasInterpreterTests {

	@Test
	public void testBasicComposeQuery() {
		PandasInterpreter pi = new PandasInterpreter();
		SelectQueryStruct sqs = new SelectQueryStruct();
		sqs.addSelector(new QueryColumnSelector("TEST__TESTID"));
		pi.setQueryStruct(sqs);
		PandasFrame pf = new PandasFrame();
		pf.setName("test12345");
		pi.setPandasFrame(pf);
		pi.setDataTypeMap(new HashMap<>());
		pi.setDataTableName("test12345", "test12345");
		String test = pi.composeQuery();
		assertEquals("test12345[['TESTID']].drop_duplicates().iloc[0:].to_dict('split')", test);
	}

}
