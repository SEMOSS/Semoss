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
package prerna.query.interpreters;

import java.util.List;
import java.util.Vector;
import org.apache.logging.log4j.Logger;
import prerna.query.querystruct.AbstractQueryStruct;

public interface IQueryInterpreter {

  /*
   * Always define these as lower case for consistency
   */

  String SEARCH_COMPARATOR = "?like";
  String NOT_SEARCH_COMPARATOR = "?nlike";
  String BEGINS_COMPARATOR = "?begins";
  String NOT_BEGINS_COMPARATOR = "?nbegins";
  String ENDS_COMPARATOR = "?ends";
  String NOT_ENDS_COMPARATOR = "?nends";

  void setQueryStruct(AbstractQueryStruct qs);

  String composeQuery();

  void setDistinct(boolean isDistinct);

  boolean isDistinct();

  void setLogger(Logger logger);

  static List<String> getAllSearchComparators() {
    return QuerySideEffect.searchComparators;
  }

  static List<String> getPosSearchComparators() {
    return QuerySideEffect.posSearchComparators;
  }

  static List<String> getNegSearchComparators() {
    return QuerySideEffect.negSearchComparators;
  }
}

// TODO :::: SHOULDN'T THIS BE MERGED WITH IQueryFilter SINCE IT HAS A LOT OF STATIC METHODS
// ALREADY???
class QuerySideEffect {

  static List<String> searchComparators = new Vector<>();

  static {
    searchComparators.add(IQueryInterpreter.SEARCH_COMPARATOR);
    searchComparators.add(IQueryInterpreter.NOT_SEARCH_COMPARATOR);
    searchComparators.add(IQueryInterpreter.BEGINS_COMPARATOR);
    searchComparators.add(IQueryInterpreter.NOT_BEGINS_COMPARATOR);
    searchComparators.add(IQueryInterpreter.ENDS_COMPARATOR);
    searchComparators.add(IQueryInterpreter.NOT_ENDS_COMPARATOR);
  }

  static List<String> posSearchComparators = new Vector<>();

  static {
    posSearchComparators.add(IQueryInterpreter.SEARCH_COMPARATOR);
    posSearchComparators.add(IQueryInterpreter.BEGINS_COMPARATOR);
    posSearchComparators.add(IQueryInterpreter.ENDS_COMPARATOR);
  }

  static List<String> negSearchComparators = new Vector<>();

  static {
    negSearchComparators.add(IQueryInterpreter.NOT_SEARCH_COMPARATOR);
    negSearchComparators.add(IQueryInterpreter.NOT_BEGINS_COMPARATOR);
    negSearchComparators.add(IQueryInterpreter.NOT_ENDS_COMPARATOR);
  }
}
