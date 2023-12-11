//package prerna.util.gson;
//
//import prerna.query.interpreters.sql.SqlInterpreter;
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.filters.SimpleQueryFilter;
//import prerna.query.querystruct.selectors.QueryColumnSelector;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//
//public class TestQuerySelectorGson {
//
//	public static void main(String[] args) {
//		
//		SelectQueryStruct qs = new SelectQueryStruct();
//		qs.addSelector(new QueryColumnSelector("Title"));
//		
//		SelectQueryStruct subQs = new SelectQueryStruct();
//		subQs.addSelector(new QueryColumnSelector("Genre__Title_FK"));
//		
//		NounMetadata sublhs = new NounMetadata(new QueryColumnSelector("Title__MovieBudget"), PixelDataType.COLUMN);
//		NounMetadata subrhs = new NounMetadata(0, PixelDataType.CONST_DECIMAL);
//		SimpleQueryFilter subQueryFilter = new SimpleQueryFilter(sublhs, ">", subrhs);
//		subQs.addExplicitFilter(subQueryFilter);
//
//		NounMetadata lhs = new NounMetadata(new QueryColumnSelector("Title__Title"), PixelDataType.COLUMN);
//		NounMetadata rhs = new NounMetadata(subQs, PixelDataType.QUERY_STRUCT);
//
//		SimpleQueryFilter filter = new SimpleQueryFilter(lhs, "==", rhs);
//		qs.addExplicitFilter(filter);
//		
//		SqlInterpreter interp = new SqlInterpreter();
//		interp.setQueryStruct(qs);
//		String sql = interp.composeQuery();
//		System.out.println(sql);
//	}
//	
//}
