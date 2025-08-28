/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MilvusVectorQueryFilterTranslationHelperUnitTests {

  private MilvusVectorQueryFitlerTranslationHelper helper;
  private String tableName;

  @BeforeEach
  void setUp() {
    helper = new MilvusVectorQueryFitlerTranslationHelper();
    tableName = "MILVUS_TABLE_NAME";
  }

  @Test
  void testProcessMilvusFilter() {
    OrQueryFilter orFilter = new OrQueryFilter();
    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", ">", 10));
    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", "<", 5));
    String filter = helper.processMilvusFilter(orFilter).toString();
    assertEquals(tableName + "__TOKEN > 10 OR " + tableName + "__TOKEN < 5", filter);

    AndQueryFilter andFilter = new AndQueryFilter();
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_LATEST", "==", true));
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_DELETED", "==", false));
    filter = helper.processMilvusFilter(andFilter).toString();
    assertEquals(
        tableName + "__IS_LATEST == true AND " + tableName + "__IS_DELETED == false", filter);

    SimpleQueryFilter simpleFilter =
        SimpleQueryFilter.makeColToColFilter(
            new QueryColumnSelector(tableName + "__COL1"),
            ">",
            new QueryColumnSelector(tableName + "__COL2"));
    filter = helper.processMilvusFilter(simpleFilter).toString();
    assertEquals(tableName + "__COL1 > '" + tableName + "__COL2'", filter);
  }

  @Test
  void testProcessOrFilter() {
    OrQueryFilter orFilter = new OrQueryFilter();
    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", ">", 10));
    orFilter.addFilter(SimpleQueryFilter.makeColToValFilter(tableName + "__TOKEN", "<", 5));
    String filter = helper.processOrQueryFilterForMilvus(orFilter).toString();
    assertEquals(tableName + "__TOKEN > 10 OR " + tableName + "__TOKEN < 5", filter);
  }

  @Test
  void testProcessAndQueryFilter() {
    AndQueryFilter andFilter = new AndQueryFilter();
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_LATEST", "==", true));
    andFilter.addFilter(
        SimpleQueryFilter.makeColToValFilter(tableName + "__IS_DELETED", "==", false));
    String filter = helper.processAndQueryFilterForMilvus(andFilter).toString();
    assertEquals(
        tableName + "__IS_LATEST == true AND " + tableName + "__IS_DELETED == false", filter);
  }

  @Test
  void testProcessSimpleQueryFilterForMilvus() {
    // COL_TO_COL
    SimpleQueryFilter simpleFilter =
        SimpleQueryFilter.makeColToColFilter(
            new QueryColumnSelector(tableName + "__COL1"),
            ">",
            new QueryColumnSelector(tableName + "__COL2"));
    String filter = helper.processSimpleQueryFilterForMilvus(simpleFilter).toString();
    assertEquals(tableName + "__COL1 > '" + tableName + "__COL2'", filter);

    // COL_TO_VAL
    simpleFilter = SimpleQueryFilter.makeColToValFilter(tableName + "__COL1", "==", 10);
    filter = helper.processSimpleQueryFilterForMilvus(simpleFilter).toString();
    assertEquals(tableName + "__COL1 == 10", filter);

    // COL_TO_QUERY
    SelectQueryStruct qs = new SelectQueryStruct();
    QueryArithmeticSelector arSel = new QueryArithmeticSelector();
    arSel.setLeftSelector(new QueryColumnSelector(tableName + "__COL1"));
    arSel.setMathExpr("*");
    arSel.setRightSelector(new QueryConstantSelector(5));
    qs.addSelector(arSel);
    simpleFilter = SimpleQueryFilter.makeColToSubQuery(tableName + "__COL", "==", qs);
    filter = helper.processSimpleQueryFilterForMilvus(simpleFilter).toString();
    assertEquals(tableName + "__COL == " + tableName + "__COL1 * 5", filter);

    // QUERY_TO_COL
    NounMetadata lnm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
    NounMetadata rnm =
        new NounMetadata(new QueryColumnSelector(tableName + "__COL"), PixelDataType.COLUMN);
    simpleFilter = new SimpleQueryFilter(lnm, "==", rnm);
    filter = helper.processSimpleQueryFilterForMilvus(simpleFilter).toString();
    assertEquals(tableName + "__COL1 * 5 == " + tableName + "__COL", filter);
  }

  @Test
  void testAddSelectorToValuesMilvusFilter() {
    NounMetadata lnm =
        new NounMetadata(new QueryColumnSelector("MILVUS_TABLE_NAME__COL1"), PixelDataType.COLUMN);
    NounMetadata rnm = new NounMetadata(10, PixelDataType.CONST_INT);
    String filter = helper.addSelectorToValuesMilvusFilter(lnm, rnm, "==").toString();
    assertEquals("MILVUS_TABLE_NAME__COL1 == 10", filter);
  }
}
