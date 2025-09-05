package util;

import prerna.auth.User;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.QueryExecutionUtility;

import java.util.List;
import java.util.Map;

public class HelperMethods {

  public static String getUserId(User user) {
    return user.getPrimaryLoginToken().getId();
  }

  // Disclaimer: This method will not work as is - it is an example of how to query a database
  public List<Map<String, Object>> queryDatabase(RDBMSNativeEngine database, String filter) {
    SelectQueryStruct qs = new SelectQueryStruct();
    qs.addSelector(new QueryColumnSelector("TABLE_1_NAME__SELECT_COLUMN_NAME"));

    qs.addRelation("TABLE_1_NAME__JOIN_COLUMN_NAME", "TABLE_2_NAME__JOIN_COLUMN_NAME", "inner.join");
    qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("TABLE_1_NAME__FILTER_COLUMN_NAME", "==", filter));

    return QueryExecutionUtility.flushRsToMap(database, qs);
  }
}
