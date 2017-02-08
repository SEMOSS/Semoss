package prerna.algorithm.learning.unsupervised.anomaly;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.r.RRoutineException;
import prerna.algorithm.learning.unsupervised.anomaly.AnomalyDetector.AnomDirection;
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

public class AnomalyDetectorTest {

	private static final String R_BASE_FOLDER = "R";
	private static final String ANALYTICS_SCRIPTS_FOLDER = "AnalyticsRoutineScripts";

	// Test case for AnomalyDetection
	public static void main(String[] args) throws Exception {
		System.out.println("----------Anomaly Detector Test----------");
		anomalyDetectorTest();
		System.out.println("------------Java Reactor Test------------");
		javaReactorTest();
	}

	private static void anomalyDetectorTest() throws RRoutineException {
		ITableDataFrame frame = makeTestFrame();

		// Create a new anomaly detector
		AnomalyDetector anomalyDetector = new AnomalyDetector(frame, "count_1", "timestamp_1", 0.01, AnomDirection.BOTH,
				0.05, 100);

		// Detect anomalies
		ITableDataFrame newFrame = anomalyDetector.detectAnomalies();

		// Final frame first row
		Iterator<IHeadersDataRow> finalData = newFrame.query("SELECT * FROM " + newFrame.getTableName() + ";");
		System.out.println(finalData.next());
	}

	private static void javaReactorTest() {
		ITableDataFrame frame = makeTestFrame();

		// Test javaReactor code
		BaseJavaReactor javaReactor = new BaseJavaReactor();
		javaReactor.dataframe = frame;
		javaReactor.runAnomalyDetection("count_1", "timestamp_1", 0.01, "both", 0.05, 100);

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
