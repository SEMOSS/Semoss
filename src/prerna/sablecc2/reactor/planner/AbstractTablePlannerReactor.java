package prerna.sablecc2.reactor.planner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import prerna.sablecc2.SimpleTable;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.TablePKSLPlanner;

public abstract class AbstractTablePlannerReactor extends AbstractReactor {

	String OPERATION_COLUMN = "OP";
	String NOUN_COLUMN = "NOUN";
	String DIRECTION_COLUMN = "DIRECTION";
	
	String inDirection = "IN";
	String outDirection = "OUT";
	
	public List<String> collectPksls(TablePKSLPlanner planner) {
		
		List<String> pksls = new ArrayList<>();
		return collectNextPksls(planner, pksls);
	}
	
	private List<String> collectNextPksls(TablePKSLPlanner planner, List<String> pksls) {
		String rootsQuery = getNextLevelQuery(pksls);
		SimpleTable table = planner.getSimpleTable();
		ResultSet results = table.executeQuery(rootsQuery);
		try {
			if(results.next()) {
				String nextPksl = results.getString(1);
				pksls.add(nextPksl);
			} else {
				return pksls;
			}
			
			while(results.next()) {
				String nextPksl = results.getString(1);
				pksls.add(nextPksl);
			}
			
			return collectNextPksls(planner, pksls);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return pksls;
	}
	
	private String getNextLevelQuery(List<String> collectedOps) {
		StringBuilder query = new StringBuilder();
		query.append("SELECT ").append(OPERATION_COLUMN);
		query.append(" FROM ");
		query.append(queryTable(collectedOps));
		query.append(" WHERE ").append(DIRECTION_COLUMN).append("=").append("'"+inDirection+"'");
		query.append(" AND ").append(NOUN_COLUMN).append(" NOT IN ");
		query.append("(").append(queryNounFilters(collectedOps)).append(")"); //this can be optimized
		return query.toString();
	}
	
	private String queryNounFilters(List<String> collectedOps) {
		StringBuilder query = new StringBuilder();
		query.append(" SELECT ").append(NOUN_COLUMN);
		query.append(" FROM ").append(queryTable(collectedOps));
		query.append(" WHERE ").append(DIRECTION_COLUMN).append("=").append("'"+outDirection+"'");
		return query.toString();
	}
	
	private String queryTable(List<String> collectedOps) {
		String query = null;
		if(collectedOps.isEmpty()) {
			query = getTableName();
		} else {
			StringBuilder q = new StringBuilder();
			q.append("SELECT ");
			q.append(this.OPERATION_COLUMN);
			q.append(", "+this.NOUN_COLUMN);
			q.append(", "+this.DIRECTION_COLUMN);
			
			q.append(" FROM ").append(getTableName());
			q.append(" WHERE "+this.OPERATION_COLUMN+" NOT IN ");
			
			//TODO: this can be optimized by adding to a table or something and using where 'not exists' in that table or something like that
			q.append("(");
			int i = 0;
			for(String op : collectedOps) {
				if(i != 0) {
					q.append(", ");
				}
				q.append(op);
				i++;
			}
			q.append(")");
		}
		
		return query;
	}
	
	private String getTableName() {
		return "";
	}

}
