//package prerna.ds.spark;
//
//import scala.collection.Iterator;
//
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//
//public class SparkFrameIterator implements java.util.Iterator<Object[]>{
//
//	Iterator sparkIterator;
//	public SparkFrameIterator(DataFrame sparkFrame) {
//		scala.collection.Iterator<Row> iterator = sparkFrame.rdd().toLocalIterator();
//		sparkIterator = iterator;
//	}
//
//	@Override
//	public boolean hasNext() {
//		return sparkIterator.hasNext();
//	}
//
//	@Override
//	public Object[] next() {
//		Row row = (Row)sparkIterator.next();
//		Object[] returnRow = new Object[row.length()];
//		for(int i = 0; i < row.length(); i++) {
//			returnRow[i] = row.get(i);
//		}
//		return returnRow;
//	}
//}
