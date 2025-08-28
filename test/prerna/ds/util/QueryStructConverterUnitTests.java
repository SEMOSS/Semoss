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
package prerna.ds.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import prerna.ds.QueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class QueryStructConverterUnitTests {

  @Test
  void testConvertOldQueryStruct() {

    QueryStruct oldQs = new QueryStruct();

    Map<String, List<String>> selectors = new Hashtable<>();
    selectors.put("table1", Arrays.asList("column1", "column2"));
    selectors.put("table2", Arrays.asList("column3", "column4"));
    oldQs.selectors = selectors;

    Map<String, Map<String, List>> andfilters = new Hashtable<>();
    Map<String, List> numericFilterMap = new Hashtable<>();
    numericFilterMap.put(">", Arrays.asList(100, 200)); // Numeric values
    andfilters.put("column1", numericFilterMap);

    Map<String, List> stringFilterMap = new Hashtable<>();
    stringFilterMap.put("=", Arrays.asList("value1", "value2")); // String values
    andfilters.put("column2", stringFilterMap);

    Map<String, List> tableFilterMap = new Hashtable<>();
    tableFilterMap.put("<", Arrays.asList(50)); // Numeric value
    andfilters.put("table1", tableFilterMap);

    oldQs.andfilters = andfilters;

    Map<String, Map<String, List>> relations = new Hashtable<>();
    Map<String, List> relationMap = new Hashtable<>();
    relationMap.put("joinType", Arrays.asList("table2"));
    relations.put("table1", relationMap);
    oldQs.relations = relations;

    oldQs.addGroupBy("table1", "column1");

    oldQs.setOrderBy("table1", "column1");

    SelectQueryStruct newQs = QueryStructConverter.convertOldQueryStruct(oldQs);

    List<QueryColumnSelector> expectedSelectors =
        Arrays.asList(
            new QueryColumnSelector("table2"),
            new QueryColumnSelector("table2__column3"),
            new QueryColumnSelector("table2__column4"),
            new QueryColumnSelector("table1"),
            new QueryColumnSelector("table1__column1"),
            new QueryColumnSelector("table1__column2"));

    assertEquals(expectedSelectors, newQs.getSelectors());

    List<QueryColumnSelector> expectedGroupBys =
        Arrays.asList(new QueryColumnSelector("table1__column1"));
    assertEquals(expectedGroupBys, newQs.getGroupBy());

    List<QueryColumnOrderBySelector> expectedOrderBys =
        Arrays.asList(new QueryColumnOrderBySelector("table1__column1"));
    assertEquals(expectedOrderBys, newQs.getOrderBy());
  }
}
