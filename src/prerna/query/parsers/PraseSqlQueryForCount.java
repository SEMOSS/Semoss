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
package prerna.query.parsers;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.util.List;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

public class PraseSqlQueryForCount {

  private static final String REPLACE_STRING = "";
  private static final String CASE_INSENSITIVE_REGEX = "(?i)";

  public String processQuery(String query) throws Exception {
    String newQuery = query;
    Pattern orderByPattern = null;
    Matcher orderByMatcher = null;

    // parse the sql
    Statement stmt = CCJSqlParserUtil.parse(query);
    Select select = ((Select) stmt);
    PlainSelect sb = (PlainSelect) select.getSelectBody();

    // get rid of order by
    {
      List<OrderByElement> orders = sb.getOrderByElements();
      if (orders != null && !orders.isEmpty()) {
        String orderExpression = PlainSelect.orderByToString(orders);

        orderByPattern = Pattern.compile(CASE_INSENSITIVE_REGEX + orderExpression);
        orderByMatcher = orderByPattern.matcher(newQuery);
        newQuery = orderByMatcher.replaceAll(REPLACE_STRING);
      }
    }

    //		// get rid of top
    //		{
    //			Top top = sb.getTop();
    //			if(top != null) {
    //				String topExpression = toRegex(top.toString());
    //				newQuery = newQuery.replaceAll(CASE_INSENSITIVE_REGEX + topExpression, "");
    //			}
    //		}
    //
    //		// get rid of limit
    //		{
    //			Limit limit = sb.getLimit();
    //			if(limit != null) {
    //				String limitExpression = toRegex(limit.toString());
    //				newQuery = newQuery.replaceAll(CASE_INSENSITIVE_REGEX + limitExpression, "");
    //			}
    //		}
    //
    //		// get rid of offset
    //		{
    //			Offset offset = sb.getOffset();
    //			if(offset != null) {
    //				String offsetExpression = toRegex(offset.toString());
    //				newQuery = newQuery.replaceAll(CASE_INSENSITIVE_REGEX + offsetExpression, "");
    //			}
    //		}
    //
    //		// get rid of fetch
    //		{
    //			Fetch fetch = sb.getFetch();
    //			if(fetch != null) {
    //				String fetchExpression = toRegex(fetch.toString());
    //				newQuery = newQuery.replaceAll(CASE_INSENSITIVE_REGEX + fetchExpression, "");
    //			}
    //		}

    return newQuery;
  }

  private static String toRegex(String s) {
    return s.replace(" ", "\\s*");
  }

  //	public static void main(String[] args) throws Exception {
  //		String query = "SELECT DISTINCT scored.County_Name_Proper AS \"County_Name_Proper\" , "
  //				+ "COUNT(DISTINCT scored.KLNK) AS \"Number_of_Individuals\" , "
  //				+ "scored.Treatment_Facilities_Size AS \"Treatment_Facilities_Size\" , "
  //				+ "scored.Drug_Death_Rate_Range AS \"Drug_Death_Rate_Range\" , "
  //				+ "scored.Population AS \"Population\" FROM scored scored  "
  //				+ "GROUP BY scored.County_Name_Proper, scored.Treatment_Facilities_Size,
  // scored.Drug_Death_Rate_Range, scored.Population "
  //				+ "ORDER BY scored.County_Name_Proper desc "
  //				+ "LIMIT 10 OFFSET 50";
  //		PraseSqlQueryForCount praser = new PraseSqlQueryForCount();
  //		System.out.println(praser.processQuery(query));
  //	}
}
