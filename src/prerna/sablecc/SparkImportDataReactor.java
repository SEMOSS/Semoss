//package prerna.sablecc;
//
//import java.util.Iterator;
//
//import prerna.ds.spark.SparkDataFrame;
//import prerna.sablecc.PKQLRunner.STATUS;
//
//public class SparkImportDataReactor extends ImportDataReactor{
//
//	@Override
//	public Iterator process() {
//		super.process();
//		
//		SparkDataFrame frame = (SparkDataFrame) myStore.get("G");
//		frame.processIterator(this.dataIterator, this.newHeaders, this.modifyNamesMap, this.joinCols, this.joinType);
//
//		inputResponseString(this.dataIterator, this.newHeaders);
//		myStore.put("STATUS", STATUS.SUCCESS);
//		
//		return null;
//	}
//}
