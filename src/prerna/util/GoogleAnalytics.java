package prerna.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.om.Insight;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.QueryStruct2.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.QueryStructConverter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;

public class GoogleAnalytics extends Thread {

	private String thisExpression = null;
	private String prevExpression = null;
	private String thisType = null;
	private String thisprevType = null;
	private String userId = null;

	public GoogleAnalytics(String thisExpression, String thisType) {
		this(thisExpression, thisType, null, null, null);
	}

	public GoogleAnalytics(String thisExpression, String thisType, String prevExpression, String prevType) {
		this(thisExpression, thisType, prevExpression, prevType, null);
	}

	public GoogleAnalytics(String thisExpression, String thisType, String prevExpression, String prevType, String userId) {
		this.thisExpression = thisExpression;
		this.prevExpression = prevExpression;
		this.thisType= thisType;
		this.thisprevType= prevType;
		this.userId = userId;
	}

	@Override
	public void run() {
		String curType = thisType;
		String prevType = thisprevType;
		String eventLabel = thisExpression;
		String previousEvent = prevExpression;
		String ID = System.getProperty("user.name");

		if (previousEvent == null){
			previousEvent = "";
		}
		if (prevType == null){
			prevType = "";
		}
		HttpClient client = HttpClientBuilder.create().build();
		//build uri to send to GA using their measurement protocol
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme("http")
		.setHost("www.google-analytics.com")
		.setPath("/collect")
		.addParameter("v", "1")
		.addParameter("t", "event")
		.addParameter("tid", "UA-99971122-1")
		.addParameter("cid", ID)
		.addParameter("cd1", curType)
		.addParameter("cd2", eventLabel)
		.addParameter("cd3", prevType)
		.addParameter("cd4", previousEvent)
		.addParameter("cd5", ID)
		.addParameter("ec", "Custom Category")
		.addParameter("ea", "Custom Action")
		.addParameter("el", "Custom Label");

		java.net.URI uri = null;
		try {
			uri = builder.build();
		} catch (URISyntaxException e) {
			return;
		}

		HttpPost post = new HttpPost(uri);
		try {
			client.execute(post);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
		}
	}

	/**
	 * Executes the tracking for an analytical routine
	 * @param in
	 * @param routine
	 */
	public static void trackAnalyticsPixel(Insight in, String routine) {
		String expression = "{\"analytics\":{\"analyticalRoutineName\":\"" + routine + "\"}}";
		in.trackPixels("analytics", expression);
	}

	/**
	 * Execute the tracking of a data import or data merge
	 * @param in
	 * @param selectors
	 */
	public static void trackDataImport(Insight in, QueryStruct2 qs) {
		final String exprStart = "{\"dataquery\":["; 
		final String exprEnd = "]}";

		String engineName = qs.getEngineName();
		if(qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY) {
			// person has entered their own query
			String query = ((HardQueryStruct) qs).getQuery();
			String expression = exprStart + 
					"{\"dbName\":\"" + engineName + "\",\"tableName\":\"null\",\"columnName\":\"null\",\"query\":\"" + query + "\"}"
					+ exprEnd;
			in.trackPixels("dataquery:", expression);
		} else {
			// person is using pixel so there is a query struct w/ selectors
			List<IQuerySelector> selectors = qs.getSelectors();
			int size = selectors.size();
			int counter = 0;
			StringBuilder exprBuilder = new StringBuilder();
			for(int i = 0; i < size; i++) {
				// loop through the selectors
				IQuerySelector s = selectors.get(i);
				if(s.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
					if(counter > 0) {
						exprBuilder.append(",");
					}
					QueryColumnSelector selector = (QueryColumnSelector) s;
					String tableName = selector.getTable();
					String columnName = selector.getColumn();
					exprBuilder.append("{\"dbName\":\"").append(engineName).append("\",\"tableName\":\"").append(tableName)
					.append("\",\"columnName\":\"").append(columnName).append("\",\"query\":\"null\"}");
					// increase counter
					counter++;
				}
			}
			in.trackPixels("dataquery", exprStart + exprBuilder.toString() + exprEnd);
		}
	}

	/**
	 * Track running and saving an existing insight
	 * @param in
	 * @param type
	 * @param engineName
	 * @param rdbmsId
	 * @param insightName
	 */
	public static void trackInsightExecution(Insight in, String type, String engineName, String rdbmsId, String insightName) {
		String curExpression = "{\"" + type + "\":{\"dbName\":\"" + engineName + "\",\"insightID\":\"" + rdbmsId + "\",\"insightName\":\"" + insightName + "\"}}";
		in.trackPixels(type, curExpression);
	}

	/**
	 * Track an excel upload into a database
	 * @param tableName
	 * @param files
	 * @param headerDataTypes
	 */
	public static void trackExcelUpload(String tableName, String fileName,
			List<Map<String, Map<String, String[]>>> headerDataTypes) {

		fileName = fileName.substring(0, fileName.length() - 24);
		final String exprStart = "{\"upload\":{\"" + fileName + "\":["; 
		final String exprEnd = "]}}";
		// String userID = request.getSession().getId();

		// if (userID != null && userID.equals("-1")) {
		// userID = null;
		// }
		StringBuilder exprBuilder = new StringBuilder();
		Map<String, Map<String, String[]>> map = headerDataTypes.get(0);
		for (Entry<String, Map<String, String[]>> entry : map.entrySet()) {
			String[] gaHeaders = map.get(entry.getKey()).get("headers");
			int counter = 0;
			for (int j = 0; j < gaHeaders.length; j++) {
				if(counter > 0) {
					exprBuilder.append(",");
				}
				exprBuilder.append("{\"dbName\":\"").append(tableName).append("\",\"columnName\":\"").append(gaHeaders[j]).append("\"}");
				counter++;
			}

			GoogleAnalytics ga = new GoogleAnalytics(exprStart + exprBuilder.toString() + exprEnd, "upload");
			// fire and release...
			ga.start();
		}

	}

	/**
	 * Track a csv upload into a database
	 * @param files
	 * @param dbName
	 * @param headerDataTypes
	 */
	public static void trackCsvUpload(String files, String dbName, List<Map<String, String[]>> headerDataTypes) {
		String fileName = files.substring(files.lastIndexOf("\\") + 1, files.lastIndexOf("."));
		fileName = fileName.substring(0, fileName.length() - 24);
		final String exprStart = "{\"upload\":{\"" + fileName + "\":["; 
		final String exprEnd = "]}}";

		StringBuilder exprBuilder = new StringBuilder();

		for (int i = 0; i < headerDataTypes.size(); i++) {
			String[] gaHeaders = headerDataTypes.get(i).get("headers");
			int counter = 0;
			for (int j = 0; j < gaHeaders.length; j++) {
				if(counter > 0) {
					exprBuilder.append(",");
				}
				exprBuilder.append("{\"dbName\":\"").append(dbName).append("\",\"columnName\":\"").append(gaHeaders[j]).append("\"}");
				counter++;
			}
			GoogleAnalytics ga = new GoogleAnalytics(exprStart + exprBuilder.toString() + exprEnd, "upload");
			// fire and release...
			ga.start();
		}
	}
	/**
	 * Track drag and drop 
	 * @param in
	 * @param headers
	 * @param FileName
	 */
	public static void trackDragAndDrop(Insight in, List<String> headers, String FileName){
		final String exprStart = "{\"draganddrop\":{\"" + FileName + "\":[{";
		final String exprEnd = "}]}}";
		StringBuilder exprBuilder = new StringBuilder();

		int count = 1;
		for (int i = 0; i < headers.size(); i++) {
			exprBuilder.append("\"columnName").append(count).append("\":\"").append(headers.get(i)).append("\"");
			if (i != (headers.size() - 1)) {
				exprBuilder.append(",");
			}
			count++;
		}
		in.trackPixels("draganddrop", exprStart + exprBuilder.toString() + exprEnd);
	}

	/**
	 * Executes tracking of visualizations
	 * @param mapOptions
	 * @param in
	 * @throws SQLException
	 */

	public static void trackViz(Map<String, Object> taskOptions, Insight in, QueryStruct2 qs) {
		if(taskOptions == null || taskOptions.isEmpty()) {
			return;
		}
		ITableDataFrame frame = (ITableDataFrame) in.getDataMaker();
		if(frame == null) {
			return;
		}
		OwlTemporalEngineMeta meta = frame.getMetaData();
		qs = QueryStructConverter.getPhysicalQs(qs, meta);

		// keep the alias to bind to the correct meta
		Map<String, String> aliasHash = new HashMap<String, String>();

		// has to be defined after qs is converted to physical
		List<IQuerySelector> selectors = qs.getSelectors();

		// loop through QS
		// figure out which selector column is part of the 
		for(int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String name = "";
			if (selector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.MATH ){
				name = ((QueryMathSelector) selector).getInnerSelector().getQueryStructName() + "";
			}else{
				name = selector.getQueryStructName();
			}
			aliasHash.put(alias, name);
		}

		for(String panelId : taskOptions.keySet()) {
			// you could be using multiple panels
			Map<String, Object> panelContent = (Map<String, Object>) taskOptions.get(panelId);

			final String exprStart = "{\"Viz\":{";
			final String exprEnd = "]}}";
			StringBuilder exprBuilder = new StringBuilder();
			String vizType = "";

			for(String panelKey : panelContent.keySet()) {
				// there are specific keys we are looking for
				// layout is the layout of the viz
				// alignment tells us what UI component this goes to
				if(panelKey.equals("layout")) {
					vizType = "\"" + panelContent.get(panelKey).toString() + "\"";
				}
				else if(panelKey.equalsIgnoreCase("alignment")) {
					// alignment points to a map of string to vector
					Map<String, List<String>> alignmentMap = (Map<String, List<String>>) panelContent.get(panelKey);
					boolean first = true;
					for(String uiCompName : alignmentMap.keySet()) {
						// ui name can be label, value, x, y, etc.
						List<String> columnsInUICompName = alignmentMap.get(uiCompName);
						// now we want to generate a map for each input in this uiCompName
						for(String columnAlias : columnsInUICompName) {
							String uniqueMetaName = aliasHash.get(columnAlias);
							List<String[]> dbInfo = meta.getDatabaseInformation(uniqueMetaName);

							if(!first) {
								exprBuilder.append(",");
							} else {
								first = false;
							}
							exprBuilder.append("[");
							int size = dbInfo.size();
							boolean processedFirst = false;
							for(int i = 0; i < size; i++) {
								String[] engineQs = dbInfo.get(i);
								if(engineQs.length != 2) {
									continue;
								}
								String db = engineQs[0];
								String conceptProp = engineQs[1];
								String table = conceptProp;
								String column = QueryStruct2.PRIM_KEY_PLACEHOLDER;
								if(conceptProp.contains("__")) {
									String[] conceptPropSplit = conceptProp.split("__");
									table = conceptPropSplit[0];
									column = conceptPropSplit[1];
								}
								if(processedFirst) {
									exprBuilder.append(",");
								} else {
									processedFirst = true;
								}
								exprBuilder.append("{\"dbName\":\"").append(db).append("\",\"tableName\": \"")
								.append(table).append("\",\"columnName\": \"").append(column)
								.append("\",\"columnType\":\"").append("categoricalTemp")
								.append("\",\"component\": \"").append(uiCompName)
								.append("\",\"numUniqueValues\":20,\"entropy\":20}");
							}
							exprBuilder.append("]");
						}
					}
				}
			}

			// we are done for this panel
			// track it
			// and then go to the next one
			String curExpression = exprStart + vizType + ":[" + exprBuilder.toString() + exprEnd;
			in.trackPixels("viz", curExpression);
		}
	}
}
