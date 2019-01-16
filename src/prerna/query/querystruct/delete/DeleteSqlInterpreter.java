package prerna.query.querystruct.delete;

import java.util.List;

import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DeleteSqlInterpreter extends SqlInterpreter {
	
	private String table;
	
	public DeleteSqlInterpreter(SelectQueryStruct qs) {
		this.qs = qs;
		this.frame = qs.getFrame();
		this.engine = qs.getEngine();
	}
	
	//////////////////////////////////////////// Compose Query //////////////////////////////////////////////
	
	public String composeQuery() {
		addTable();
		addFilters();
		
		StringBuilder query = new StringBuilder("DELETE FROM ");
		query.append(table);
		
		int numFilters = this.filterStatements.size();
		for(int i = 0; i < numFilters; i++) {
			if(i == 0) {
				query.append(" WHERE ");
			} else {
				query.append(" AND ");
			}
			query.append(this.filterStatements.get(i).toString());
		}
		
		return query.toString();
	}
	
	/**
	 * This is set directly in the QueryDelete reactor
	 */
	public void addTable() {
		List<IQuerySelector> selectors = qs.getSelectors();
		QueryColumnSelector t = (QueryColumnSelector) selectors.get(0);
		this.table = t.getTable();
	}
	
	public static void main(String[] args) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector("table", "column");
		QueryColumnSelector tab = new QueryColumnSelector("Nominated__Title_FK");
		QueryColumnSelector tab2 = new QueryColumnSelector("Nominated__Revenue");
		NounMetadata fil1 = new NounMetadata(tab, PixelDataType.COLUMN);
		NounMetadata fil2 = new NounMetadata("Chocolat", PixelDataType.CONST_STRING);
		NounMetadata fil3 = new NounMetadata(tab2, PixelDataType.COLUMN);
		NounMetadata fil4 = new NounMetadata(300000, PixelDataType.CONST_INT);
		SimpleQueryFilter filter1 = new SimpleQueryFilter(fil2, "=", fil1);
//		SimpleQueryFilter filter2 = new SimpleQueryFilter(fil4, "=", fil3);
		qs.addExplicitFilter(filter1);
//		qs.addExplicitFilter(filter2);
		DeleteSqlInterpreter interpreter = new DeleteSqlInterpreter(qs);
		String s = interpreter.composeQuery();
		System.out.println(s);
	}
	
}
