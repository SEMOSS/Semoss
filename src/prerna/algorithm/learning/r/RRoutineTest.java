package prerna.algorithm.learning.r;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.rosuda.REngine.REXP;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutine.Builder.RRoutineType;
import prerna.ds.QueryStruct;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;
import prerna.ds.util.FileIterator;
import prerna.ds.util.FileIterator.FILE_DATA_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.BaseJavaReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RRoutineTest {

	private static final String R_BASE_FOLDER = "R";
	private static final String ANALYTICS_SCRIPTS_FOLDER = "AnalyticsRoutineScripts";

	// Test case for RRoutine
	public static void main(String[] args) throws Exception {
		System.out.println("------------RRoutine Test------------");
		rRoutineTest();
		System.out.println("----------Java Reactor Test----------");
		javaReactorTest();
	}

	private static void rRoutineTest() throws Exception {
		ITableDataFrame frame = makeTestFrame();
		RRoutine rRoutine = new RRoutine.Builder(frame, "AnomalyDetection.R", "df")
				.selectedColumns("timestamp_1;count_1").arguments("'count_1';0.01;'both';0.05;100;FALSE")
				.routineType(RRoutineType.ANALYTICS).build();

		ITableDataFrame newFrame = rRoutine.returnDataFrame();
		REXP result = rRoutine.returnResult();

		// Final frame first row
		Iterator<IHeadersDataRow> finalData = newFrame.query("SELECT * FROM " + newFrame.getTableName() + ";");
		System.out.println(finalData.next());

		// Result
		System.out.println(result.asInteger());
	}

	private static void javaReactorTest() {
		ITableDataFrame frame = makeTestFrame();

		// Test javaReactor code
		BaseJavaReactor javaReactor = new BaseJavaReactor();
		javaReactor.dataframe = frame;
		javaReactor.runRRoutine("AnomalyDetection.R", "df", "timestamp_1;count_1", "df",
				"'COUNT_1';0.01;'both';0.05;100;FALSE");

		// Final frame first row
		Iterator<IHeadersDataRow> finalData = javaReactor.dataframe
				.query("SELECT * FROM " + javaReactor.dataframe.getTableName() + ";");
		System.out.println(finalData.next());
	}

	private static ITableDataFrame makeTestFrame() {
		DIHelper.getInstance().loadCoreProp(System.getProperty("user.dir") + "/RDF_Map.prop");

		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String scriptsDirectory = baseDirectory + "\\" + R_BASE_FOLDER + "\\" + ANALYTICS_SCRIPTS_FOLDER;

		// Create an H2Frame for testing
		H2Frame frame = new H2Frame();

		String fileName = scriptsDirectory + "\\anomaly_detection_test.csv";

		String[] colNames = new String[3];
		colNames[0] = "index_1";
		colNames[1] = "timestamp_1";
		colNames[2] = "count_1";

		String[] colTypes = new String[3];
		colTypes[0] = "NUMBER";
		colTypes[1] = "STRING";
		colTypes[2] = "NUMBER";

		// Need to create a data type map and a query structure
		QueryStruct qs = new QueryStruct();
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new Hashtable<String, IMetaData.DATA_TYPES>();
		Map<String, String> dataTypeMapStr = new Hashtable<String, String>();
		for (int i = 0; i < colNames.length; i++) {
			dataTypeMapStr.put(colNames[i], colTypes[i]);
			dataTypeMap.put(colNames[i], Utility.convertStringToDataType(colTypes[i]));
			qs.addSelector(colNames[i], null);
		}

		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(colNames);
		frame.mergeEdgeHash(edgeHash, dataTypeMapStr);

		// Iterate through file and insert values
		FileIterator dataIterator = FileIterator.createInstance(FILE_DATA_TYPE.META_DATA_ENUM, fileName, ',', qs,
				dataTypeMap);
		frame.addRowsViaIterator(dataIterator, dataTypeMap);

		// Update the user id to match the new schema
		frame.setUserId(frame.getSchema());

		// Original frame first row
		Iterator<IHeadersDataRow> originalData = frame.query("SELECT * FROM " + frame.getTableName() + ";");
		System.out.println(originalData.next());
		return frame;
	}

}
