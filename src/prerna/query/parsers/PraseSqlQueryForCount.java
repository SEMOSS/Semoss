package prerna.query.parsers;

import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Fetch;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.Top;

public class PraseSqlQueryForCount {

	public String processQuery(String query) throws Exception {
		String newQuery = query;

		// parse the sql
		Statement stmt = CCJSqlParserUtil.parse(query);
		Select select = ((Select)stmt);
		PlainSelect sb = (PlainSelect)select.getSelectBody();
		
		// get rid of order by
		{
			List<OrderByElement> orders = sb.getOrderByElements();
			if(orders != null && !orders.isEmpty()) {
				newQuery = newQuery.replaceAll("(?i)order\\s*by", "");
				for(OrderByElement order : orders) {
					String orderExpression = toRegex(order.toString());
					newQuery = newQuery.replaceAll("(?i)" + orderExpression, "");
				}
			}
		}
		
		// get rid of top
		{
			Top top = sb.getTop();
			if(top != null) {
				String topExpression = toRegex(top.toString());
				newQuery = newQuery.replaceAll("(?i)" + topExpression, "");
			}
		}
		
		// get rid of limit
		{
			Limit limit = sb.getLimit();
			if(limit != null) {
				String limitExpression = toRegex(limit.toString());
				newQuery = newQuery.replaceAll("(?i)" + limitExpression, "");
			}
		}
		
		// get rid of offset
		{
			Offset offset = sb.getOffset();
			if(offset != null) {
				String offsetExpression = toRegex(offset.toString());
				newQuery = newQuery.replaceAll("(?i)" + offsetExpression, "");
			}
		}
		
		// get rid of fetch
		{
			Fetch fetch = sb.getFetch();
			if(fetch != null) {
				String fetchExpression = toRegex(fetch.toString());
				newQuery = newQuery.replaceAll("(?i)" + fetchExpression, "");
			}
		}
		
		return newQuery;
	}
	
	private static String toRegex(String s) {
		return s.replace(" ", "\\s*");
	}
	
	public static void main(String[] args) throws Exception {
		String query = "SELECT DISTINCT scored.County_Name_Proper AS \"County_Name_Proper\" , "
				+ "COUNT(DISTINCT scored.KLNK) AS \"Number_of_Individuals\" , "
				+ "scored.Treatment_Facilities_Size AS \"Treatment_Facilities_Size\" , "
				+ "scored.Drug_Death_Rate_Range AS \"Drug_Death_Rate_Range\" , "
				+ "scored.Population AS \"Population\" FROM scored scored  "
				+ "GROUP BY scored.County_Name_Proper, scored.Treatment_Facilities_Size, scored.Drug_Death_Rate_Range, scored.Population "
				+ "ORDER BY scored.County_Name_Proper desc "
				+ "LIMIT 10 OFFSET 50";
		PraseSqlQueryForCount praser = new PraseSqlQueryForCount();
		System.out.println(praser.processQuery(query));
	}
}
