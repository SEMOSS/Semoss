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
package prerna.querystruct;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.om.Insight;
import prerna.query.querystruct.SetParamsReactor;
import prerna.query.parsers.ParamStruct;
import prerna.query.parsers.ParamStructDetails;
import prerna.reactor.insights.recipemanagement.ImportParamOptionsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetParamsReactorUnitTests {
	private SetParamsReactor reactor;
	private Insight insight;
	private Map<String, String> keyValues;
	private Map<String, Object> pixelMap = new HashMap<>();
	private Map<String, Object> paramMap;
	private Map<String, Object> columnMap;
	private Map<String, Object> tableMap;
	private Map<String, Object> operatorMap;
	private Map<String, Object> operatorUMap;
	private List<ParamStruct> list;
	private ParamStruct struct;

    @BeforeEach
	void setup() {
		reactor = new SetParamsReactor();
		keyValues = reactor.keyValue;
		insight = mock(Insight.class);
		reactor.setInsight(insight);

		struct = new ParamStruct();
		struct.addParamStructDetails(new ParamStructDetails());
		paramMap = new HashMap<>();
		columnMap = new HashMap<>();
		tableMap = new HashMap<>();
		operatorMap = new HashMap<>();
		operatorUMap = new HashMap<>();
		list = new ArrayList<ParamStruct>();
	}

	@Test
	void executeNoParams() {
		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(null);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("There is no params available to modify ", nm.getValue().toString());
	}

	@Test
	void executeNoPixekId() {
		Map<String, Object> map = new HashMap<>();
		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(map);
		
		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "column");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No such pixel ", nm.getValue().toString());
	}

	@Test
	void executeNoTable () {
		pixelMap.put("testPixelId", new HashMap());

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "table");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column or table key specified ", nm.getValue().toString());
	}

	@Test
	void executeNoOperator () {
		pixelMap.put("testPixelId", new HashMap());

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operator");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column, table or operator key specified ", nm.getValue().toString());
	}
	
	@Test
	void executeNoOperatorU () {
		pixelMap.put("testPixelId", new HashMap());

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operatoru");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");
		keyValues.put(ReactorKeysEnum.OPERATOR.getKey(), "testOperator");

		NounMetadata nm = reactor.execute();
		assertNotNull(nm);
		assertEquals(PixelDataType.ERROR, nm.getNounType());
		assertEquals("No column, table, operator or unique operator key specified ", nm.getValue().toString());
	}

	@Test
	void executeWithColumn () {
		list = new ArrayList<ParamStruct>();
		list.add(struct);
		
		operatorUMap.put("testOperatorU", list);
		operatorMap.put("testOperator", operatorUMap);
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "column");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeWithTable () {
		list = new ArrayList<ParamStruct>();
		list.add(struct);
		
		operatorUMap.put("testOperatorU", list);
		operatorMap.put("testOperator", operatorUMap);
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "table");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeWithOperator () {
		list = new ArrayList<ParamStruct>();
		list.add(struct);
		
		operatorUMap.put("testOperatorU", list);
		operatorMap.put("testOperator", operatorUMap);
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operator");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");
		keyValues.put(ReactorKeysEnum.OPERATOR.getKey(), "testOp");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeWithOperatorU () {
		list = new ArrayList<ParamStruct>();
		list.add(struct);
		
		operatorUMap.put("testOperatorU", list);
		operatorMap.put("testOperator", operatorUMap);
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operatoru");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");
		keyValues.put(ReactorKeysEnum.OPERATOR.getKey(), "testOperator");
		keyValues.put(ReactorKeysEnum.OPERATORU.getKey(), "testOperatorU");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeColumnNotInMap () {
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "column");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeTableNotInMap () {
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "table");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeOperatorNotInMap () {
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operator");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");
		keyValues.put(ReactorKeysEnum.OPERATOR.getKey(), "testOp");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

	@Test
	void executeOperatorUNotInMap () {
		operatorMap.put("testOperator", operatorUMap);
		tableMap.put("testTable", operatorMap);
		paramMap.put("testCol", tableMap);
		pixelMap.put("testPixelId", paramMap);

		when(insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS)).thenReturn(pixelMap);

		keyValues.put(ReactorKeysEnum.PIXEL_ID.getKey(), "testPixelId");
		keyValues.put(ReactorKeysEnum.ID_TYPE.getKey(), "operatoru");
		keyValues.put(ReactorKeysEnum.VALUE.getKey(), "testVal");
		keyValues.put(ReactorKeysEnum.COLUMN.getKey(), "testCol");
		keyValues.put(ReactorKeysEnum.TABLE.getKey(), "testTable");
		keyValues.put(ReactorKeysEnum.OPERATOR.getKey(), "testOperator");
		keyValues.put(ReactorKeysEnum.OPERATORU.getKey(), "testOperatorU");

		NounMetadata nm = reactor.execute();

		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
		assertEquals("Parameters set ", nm.getValue().toString());
	}

}
