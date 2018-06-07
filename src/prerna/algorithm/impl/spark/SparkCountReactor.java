//package prerna.algorithm.impl.spark;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.ds.spark.SparkDataFrame;
//
//public class SparkCountReactor extends SparkBaseReducerReactor{
//
//	@Override
//	public Object reduce() {
//		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
//		return ((SparkDataFrame)frame).mapReduce(columns, groupBys, "COUNT", expr);	
//	}
//}
