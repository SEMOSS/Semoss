//
//package prerna.ds.spark;
//
//import scala.collection.Iterator;
//
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//
//public class UniqueValueSparkFrameIterator implements java.util.Iterator<Object>{
//
//	Iterator sparkIterator;
//	public UniqueValueSparkFrameIterator(DataFrame sparkFrame) {
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
//	public Object next() {
//		return ((Row)sparkIterator.next()).get(0);
//	}
//}
