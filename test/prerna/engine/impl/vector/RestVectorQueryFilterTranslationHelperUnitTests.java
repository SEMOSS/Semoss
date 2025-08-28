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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RestVectorQueryFilterTranslationHelperUnitTests {

  private String tableName;
  private AndQueryFilter andFilter;
  private OrQueryFilter orFilter;
  private BetweenQueryFilter betweenFilter;
  private FunctionQueryFilter functionfilter;

  @BeforeEach
  void setUp() {
    tableName = "TEST_TABLE";
    andFilter = new AndQueryFilter();
    orFilter = new OrQueryFilter();
    betweenFilter = new BetweenQueryFilter();
    functionfilter = new FunctionQueryFilter();
  }

  @Test
  void testProcessFilter() {
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_LATEST", "==", true));
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_DELETED", "==", false));

    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", ">", 10));
    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", "<=", 5));

    SimpleQueryFilter simpleFilter =
        SimpleQueryFilter.makeColToValFilter(tableName + "__SOURCE", "==", "sourceDoc");
    JsonArray filter = new JsonArray();
    JsonArray should = new JsonArray();
    JsonArray must_not = new JsonArray();
    assertTrue(filter.isEmpty());
    assertTrue(should.isEmpty());
    assertTrue(must_not.isEmpty());
    RestVectorQueryFilterTranslationHelper.processFilter(simpleFilter, filter, should, must_not);
    assertEquals(1, filter.size());
    JsonObject filterValue = filter.get(0).getAsJsonObject().get("match").getAsJsonObject();
    assertFalse(filterValue.isEmpty());
    for (String key : filterValue.keySet()) {
      assertEquals(tableName + "__SOURCE", key);
      assertEquals("sourceDoc", filterValue.get(key).getAsString());
    }
    assertTrue(should.isEmpty());
    assertTrue(must_not.isEmpty());

    // testing AND filters
    filter = new JsonArray();
    should = new JsonArray();
    must_not = new JsonArray();
    assertTrue(filter.isEmpty());
    assertTrue(should.isEmpty());
    assertTrue(must_not.isEmpty());
    RestVectorQueryFilterTranslationHelper.processFilter(andFilter, filter, should, must_not);
    assertEquals(2, filter.size());
    filterValue = filter.get(0).getAsJsonObject().get("match").getAsJsonObject();
    assertFalse(filterValue.isEmpty());
    for (String key : filterValue.keySet()) {
      assertEquals(tableName + "__IS_LATEST", key);
      assertTrue(filterValue.get(key).getAsBoolean());
    }
    filterValue = filter.get(1).getAsJsonObject().get("match").getAsJsonObject();
    assertFalse(filterValue.isEmpty());
    for (String key : filterValue.keySet()) {
      assertEquals(tableName + "__IS_DELETED", key);
      assertFalse(filterValue.get(key).getAsBoolean());
    }
    assertTrue(should.isEmpty());
    assertTrue(must_not.isEmpty());

    // OR filters
    filter = new JsonArray();
    should = new JsonArray();
    must_not = new JsonArray();
    assertTrue(filter.isEmpty());
    assertTrue(should.isEmpty());
    assertTrue(must_not.isEmpty());
    RestVectorQueryFilterTranslationHelper.processFilter(orFilter, filter, should, must_not);
    assertTrue(filter.isEmpty());
    JsonObject shouldVal = should.get(0).getAsJsonObject().get("range").getAsJsonObject();
    for (String key : shouldVal.keySet()) {
      assertEquals(tableName + "__TOKEN", key);
      JsonObject compareMap = shouldVal.get(key).getAsJsonObject();
      assertEquals(10, compareMap.get("gte").getAsInt());
    }
    assertTrue(must_not.isEmpty());
  }

  @Test
  void testProcessFilterInvalidFilter() {
    IllegalArgumentException betweenFilterErr =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RestVectorQueryFilterTranslationHelper.processFilter(
                    betweenFilter, null, null, null));
    assertEquals(
        "Filters with a Query Filter Type of Between are not supported for Elastic Search vector databases",
        betweenFilterErr.getMessage());

    IllegalArgumentException functionFilterErr =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RestVectorQueryFilterTranslationHelper.processFilter(
                    functionfilter, null, null, null));
    assertEquals(
        "Filters with a Query Filter Type of Function are not supported for Elastic Search vector databases",
        functionFilterErr.getMessage());
  }

  @Test
  void testProcessSimpleQueryFilter() {
    String col1 = "COL1";
    int val1 = 10;
    SimpleQueryFilter filter =
        SimpleQueryFilter.makeColToValFilter(tableName + "__" + col1, ">", val1);
    JsonObject output = RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter);
    JsonObject rangeVal = output.get("range").getAsJsonObject();
    for (String key : rangeVal.keySet()) {
      assertEquals(tableName + "__" + col1, key);
      JsonObject compareMap = rangeVal.get(key).getAsJsonObject();
      assertEquals(val1, compareMap.get("gte").getAsInt());
    }
  }

  @Test
  void testProcessSimpleQueryFilterInvalidFilterTypes() {
    // COL_TO_COL filter
    SimpleQueryFilter filter =
        SimpleQueryFilter.makeColToColFilter(tableName + "__COL1", "==", tableName + "__COL2");

    IllegalArgumentException colToColErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter));
    assertEquals(
        "Filter of with a Filter Type of COL_TO_COL are not supported for Elastic/Open Search vector databases",
        colToColErr.getMessage());

    // COL_TO_QUERY filter
    SimpleQueryFilter filter1 =
        SimpleQueryFilter.makeColToSubQuery(tableName + "__COL", "==", new SelectQueryStruct());

    IllegalArgumentException colToQueryErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter1));
    assertEquals(
        "Filter of with a Filter Type of COL_TO_QUERY are not supported for Elastic/Open Search vector databases",
        colToQueryErr.getMessage());

    // QUERY_TO_COL filter
    NounMetadata lnm = new NounMetadata(new SelectQueryStruct(), PixelDataType.QUERY_STRUCT);
    NounMetadata rnm =
        new NounMetadata(new QueryColumnSelector(tableName + "__COL"), PixelDataType.COLUMN);
    SimpleQueryFilter filter2 = new SimpleQueryFilter(lnm, "==", rnm);

    IllegalArgumentException queryToColErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter2));
    assertEquals(
        "Filter of with a Filter Type of QUERY_TO_COL are not supported for Elastic/Open Search vector databases",
        queryToColErr.getMessage());

    // COL_TO_LAMBDA filter
    lnm = new NounMetadata(new QueryColumnSelector(tableName + "__COL"), PixelDataType.COLUMN);
    rnm = new NounMetadata("String", PixelDataType.LAMBDA);
    SimpleQueryFilter filter3 = new SimpleQueryFilter(lnm, "==", rnm);

    IllegalArgumentException colToLambdaErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter3));
    assertEquals(
        "Filter of with a Filter Type of COL_TO_LAMBDA are not supported for Elastic/Open Search vector databases",
        colToLambdaErr.getMessage());

    // LAMBDA_TO_COL filter
    lnm = new NounMetadata("String", PixelDataType.LAMBDA);
    rnm = new NounMetadata(new QueryColumnSelector(tableName + "__COL"), PixelDataType.COLUMN);
    SimpleQueryFilter filter4 = new SimpleQueryFilter(lnm, "==", rnm);

    IllegalArgumentException lambdaToColErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter4));
    assertEquals(
        "Filter of with a Filter Type of LAMBDA_TO_COL are not supported for Elastic/Open Search vector databases",
        lambdaToColErr.getMessage());

    // VAL_TO_VAL filter
    lnm = new NounMetadata(1, PixelDataType.CONST_INT);
    rnm = new NounMetadata(1, PixelDataType.CONST_INT);
    SimpleQueryFilter filter5 = new SimpleQueryFilter(lnm, "==", rnm);

    IllegalArgumentException valToValErr =
        assertThrows(
            IllegalArgumentException.class,
            () -> RestVectorQueryFilterTranslationHelper.processSimpleQueryFilter(filter5));
    assertEquals(
        "Filter of with a Filter Type of VALUE_TO_VALUE are not supported for Elastic/Open Search vector databases",
        valToValErr.getMessage());
  }
}
