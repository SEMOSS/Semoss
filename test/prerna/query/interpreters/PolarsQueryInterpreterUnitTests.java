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
package prerna.query.interpreters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class PolarsQueryInterpreterUnitTests {

	private static final Gson GSON = new Gson();

	@Test
	void buildsNativePlanForSelectorsFiltersAndPaging() {
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		queryStruct.addSelector(new QueryColumnSelector("frame__name", "label"));
		queryStruct.addSelector(QueryArithmeticSelector.makeCol2ColSelector(
				new QueryColumnSelector("frame__value"),
				new QueryConstantSelector(2),
				"*",
				"doubled"));
		queryStruct.addExplicitFilter(new AndQueryFilter(
				new SimpleQueryFilter(
						new NounMetadata(new QueryColumnSelector("frame__value"), PixelDataType.COLUMN),
						">",
						new NounMetadata(2, PixelDataType.CONST_INT)),
				new SimpleQueryFilter(
						new NounMetadata(new QueryColumnSelector("frame__name"), PixelDataType.COLUMN),
						"==",
						new NounMetadata(List.of("alpha", "beta"), PixelDataType.VECTOR))));
		queryStruct.addOrderBy("frame__value", "DESC");
		queryStruct.setOffSet(1);
		queryStruct.setLimit(5);

		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setQueryStruct(queryStruct);

		Map<?, ?> plan = GSON.fromJson(interpreter.composeQuery(), Map.class);
		assertEquals(2, ((List<?>) plan.get("selectors")).size());
		assertEquals("and", ((Map<?, ?>) plan.get("filter")).get("kind"));
		assertEquals(1.0, plan.get("offset"));
		assertEquals(5.0, plan.get("limit"));
		assertEquals(Boolean.TRUE, ((Map<?, ?>) ((List<?>) plan.get("sort")).get(0)).get("descending"));
	}

	@Test
	void buildsGroupedAggregatePlan() {
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		QueryColumnSelector group = new QueryColumnSelector("frame__category", "category");
		queryStruct.addSelector(group);
		queryStruct.addGroupBy(group);
		QueryFunctionSelector sum = QueryFunctionSelector.makeFunctionSelector(
				QueryFunctionHelper.SUM, "frame__amount", "total");
		queryStruct.addSelector(sum);

		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setQueryStruct(queryStruct);
		Map<?, ?> plan = GSON.fromJson(interpreter.composeQuery(), Map.class);

		assertEquals(1, ((List<?>) plan.get("groupBy")).size());
		Map<?, ?> aggregate = (Map<?, ?>) ((List<?>) plan.get("selectors")).get(1);
		assertEquals("function", aggregate.get("kind"));
		assertEquals(QueryFunctionHelper.SUM, aggregate.get("function"));
	}

	@Test
	void ignoresPrimaryKeyPlaceholderSorts() {
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		queryStruct.addSelector(QueryFunctionSelector.makeFunctionSelector(
				QueryFunctionHelper.SUM, "frame__amount", "total"));
		queryStruct.addOrderBy("frame", SelectQueryStruct.PRIM_KEY_PLACEHOLDER, "ASC");

		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setQueryStruct(queryStruct);
		Map<?, ?> plan = GSON.fromJson(interpreter.composeQuery(), Map.class);

		assertEquals(0, ((List<?>) plan.get("sort")).size());
	}

	@Test
	void rejectsUnsupportedSelectorsBeforeExecution() {
		SelectQueryStruct queryStruct = new SelectQueryStruct();
		prerna.query.querystruct.selectors.QueryOpaqueSelector opaque =
				new prerna.query.querystruct.selectors.QueryOpaqueSelector("arbitrary()");
		queryStruct.addSelector(opaque);

		PolarsQueryInterpreter interpreter = new PolarsQueryInterpreter();
		interpreter.setQueryStruct(queryStruct);
		assertThrows(IllegalArgumentException.class, interpreter::composeQuery);
	}
}
