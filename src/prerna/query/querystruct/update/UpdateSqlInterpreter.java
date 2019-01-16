package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateSqlInterpreter extends SqlInterpreter {
	
	private UpdateQueryStruct qs;
	
	private StringBuilder sets = new StringBuilder();
	private StringBuilder updateFrom = new StringBuilder();
	
	public UpdateSqlInterpreter(UpdateQueryStruct qs) {
		this.qs = qs;
		this.frame = qs.getFrame();
		this.engine = qs.getEngine();
	}
	
	//////////////////////////////////////////// Compose Query //////////////////////////////////////////////

	public String composeQuery() {
		addSelectors();
		addSets();
		addFilters();
		
		// Initiate String
		StringBuilder query = new StringBuilder("UPDATE ");
		// Add sets depending on...
		query.append(this.updateFrom);
		query.append(" SET ").append(this.sets);
		
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
	
	//////////////////////////////////////////// End Compose Query //////////////////////////////////////////
	
	//////////////////////////////////////////// Add Selectors //////////////////////////////////////////////
	
	@Override
	public void addSelectors() {
		List<IQuerySelector> selectorData = qs.getSelectors();
		Set<String> tableList = new HashSet<String>();

		for(IQuerySelector selector : selectorData) {
			if(selector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				throw new IllegalArgumentException("Can only update column values");
			}

			QueryColumnSelector t = (QueryColumnSelector) selector;
			String table = t.getTable();
			tableList.add(table);
		}		
		
		Iterator<String> it = tableList.iterator();
		if(it.hasNext()) {
			this.updateFrom.append(it.next());
		}
		while(it.hasNext()) {
			this.updateFrom.append(", ").append(it.next());
		}
	}

	private void addSets() {
		List<IQuerySelector> selectors = qs.getSelectors();
		List<Object> values = qs.getValues();
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			if(i != 0) {
				sets.append(", ");
			}
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
			String table = s.getTable();
			String column = s.getColumn();
			if(column.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				column = getPrimKey4Table(table);
			}
			Object v = values.get(i);
			if(v instanceof String) {
				sets.append(table + "." + column + "=" + "'" + RdbmsQueryBuilder.escapeForSQLStatement(v + "") + "'");
			} else if(v instanceof SemossDate) {
				sets.append(table + "." + column + "=" + "'" + ((SemossDate) v).getFormattedDate() + "'");
			} else {
				sets.append(table + "." + column + "=" + v );
			}
		}
	}
	

	//////////////////////////////////////////// Main function to test //////////////////////////////////////
	
	public static void main(String[] args) {
		// load engine
//		TestUtilityMethods.loadDIHelper("C:/Users/laurlai/workspace/Semoss/RDF_Map.prop");
//		
//		String engineProp = "C:/Users/laurlai/workspace/Semoss/db/LocalMasterDatabase.smss";
//		IEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
//
//		engineProp = "C:/Users/laurlai/workspace/Semoss/db/MovieDB.smss";
//		coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName("MovieDB");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("MovieDB", coreEngine);
		
		
		// Create qs object
		UpdateQueryStruct qs = new UpdateQueryStruct();
		
		/**
		 * Update one column on one table
		 */
		qs.addSelector("Nominated", "Nominated");
		List<Object> values = new ArrayList<Object>();
		values.add("N");
		qs.setValues(values);
		QueryColumnSelector tab = new QueryColumnSelector("Nominated__Title_FK");
		NounMetadata fil1 = new NounMetadata(tab, PixelDataType.COLUMN);
		NounMetadata fil2 = new NounMetadata("Chocolat", PixelDataType.CONST_STRING);
		SimpleQueryFilter filter1 = new SimpleQueryFilter(fil2, "=", fil1);
		qs.addExplicitFilter(filter1);
		
		/**
		 * Update one table using values of another for reference
		 * UPDATE Genre SET Genre.Genre='Comedy' WHERE Genre.Title_FK IN (SELECT Title.Title FROM Title WHERE Title = 'Avatar')
		 */
//		qs.addSelector("Genre", "Genre");
//		List<Object> values = new ArrayList<Object>();
//		values.add("Drama");
//		qs.setValues(values);
//		
//		// Making subquery
//		QueryStruct2 subQuery = new QueryStruct2();
//		QueryColumnSelector title = new QueryColumnSelector("Title__Title");
//		subQuery.addSelector(title);
//		NounMetadata fil3 = new NounMetadata(title, PixelDataType.COLUMN);
//		NounMetadata fil4 = new NounMetadata("Avatar", PixelDataType.CONST_STRING);
//		SimpleQueryFilter subQueryFilter = new SimpleQueryFilter(fil3, "=", fil4);
//		subQuery.addExplicitFilter(subQueryFilter);
//		
////		// Add to qs
//		NounMetadata col = new NounMetadata(new QueryColumnSelector("Genre__Title_FK"), PixelDataType.COLUMN);
//		NounMetadata filquery = new NounMetadata(subQuery, PixelDataType.QUERY_STRUCT);
//		SimpleQueryFilter filter5 = new SimpleQueryFilter(col, "==", filquery);
//		qs.addExplicitFilter(filter5);
//				
		// Create interpreter and compose query
		UpdateSqlInterpreter interpreter = new UpdateSqlInterpreter(qs);
		String s = interpreter.composeQuery();
		System.out.println(s);
		
		// run query on engine
//		coreEngine.insertData(s);
//		
//		// viewing results
////		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Nominated");
//		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Genre");
//		while(it.hasNext()) {
//			System.out.println(Arrays.toString(it.next().getValues()));
//		}
	}
	
	
}
