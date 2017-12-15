package prerna.util;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;

import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;
import prerna.query.querystruct.HardQueryStruct;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.QueryStruct2.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

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
		String ID;
		ID = System.getProperty("user.name");

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
		System.out.println("GOOGLE ANALYTICS: "+uri);
		HttpPost post = new HttpPost(uri);
		try {
			client.execute(post);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
		}
	}

	public String parseViz(List<Object> mapOptions, Insight insight) throws SQLException {

		HashMap<String, Object> content = (HashMap<String, Object>) ((HashMap<String, Object>) mapOptions.get(0)).get("0");
		String table = null;
		String database = null;
		HashMap<String, String> myHash = new HashMap<String, String>();
		List<IQuerySelector> selectorList = null;
		Object rawFrame = insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
		
		if (rawFrame instanceof H2Frame) {
			H2Frame frame = (H2Frame) insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
			table = frame.getTableName();
			Map<String, Object> meta = frame.getMetaData().getTableHeaderObjects(false);
			String test = frame.getViewTableName();
			myHash = createHash(meta);
		} else if (rawFrame instanceof RDataTable) {
			RDataTable frame = (RDataTable) insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
			table = frame.getTableName();
			Map<String, Object> meta = frame.getMetaData().getTableHeaderObjects(false);
			myHash = createHash(meta);
		} else if (rawFrame instanceof TinkerFrame) {
			TinkerFrame frame = (TinkerFrame) insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
			table = frame.getTableName();
			Map<String, Object> meta = frame.getMetaData().getTableHeaderObjects(false);
			myHash = createHash(meta);
		} else if (rawFrame instanceof NativeFrame) {
			NativeFrame frame = (NativeFrame) insight.getVarStore().get("$CUR_FRAME_KEY").getValue();
			table = frame.getTableName();
			Map<String, Object> meta = frame.getMetaData().getTableHeaderObjects(false);
			selectorList = ((NativeFrame) rawFrame).getQueryStruct().getSelectors();
			myHash = createHash(meta);
		}

		String curExpression = "{\"Viz\":{\"";
		if ((content == null)) {
			return "{\"Viz\":{\"CollisionResolver\": [{\"dbName\": \"null\",\"tableName\": \"null\",\"columnName\": \"null\",\"columnType\": \"categorical\",\"component\": \"null\",\"numUniqueValues\": 25,\"entropy\": 0.1}]}}";
		} else {
			Iterator<Entry<String, Object>> it = content.entrySet().iterator();
			Object key = null;
			Object value = null;
			int entryNum = 1;
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				key = pair.getKey();
				value = content.get(key);
				String columnType = "categoricalTemp";
				if (key.equals("layout")) {
					curExpression += value + "\":[{";
				} else if (key.equals("alignment") && !(key.toString().isEmpty())) {
					try {
						HashMap<String, Object> alignValue = (HashMap<String, Object>) value;
						Iterator<Entry<String, Object>> it2 = alignValue.entrySet().iterator();
						// iterate through tooltip/label/values
						while (it2.hasNext()) {
							Map.Entry pair2 = (Map.Entry) it2.next();
							// tooltip
							key = pair2.getKey();
							@SuppressWarnings("unchecked")
							Vector<String> value2 = (Vector<String>) alignValue.get(key);
							if (value2.size() > 0) {
								if (entryNum != 1) {
									curExpression += ",{";
								}
								for (int i = 0; i < value2.size(); i++) {
									String column = value2.get(i) + "";
									if (column.startsWith("Countof")) {
										column = column.replaceFirst("Countof", "");
									} else if (column.startsWith("Sumof")) {
										column = column.replaceFirst("Sumof", "");
									} else if (column.startsWith("UniqueGroupConcatof")) {
										column = column.replaceFirst("UniqueGroupConcatof", "");
									}
									database = (String) myHash.get(column);
									if (column.contains("__")) {
										String[] array = column.split("__");
										column = array[1];
										table = array[0];
									}
									// update table name if native or H2/R Temp frame
									if (rawFrame instanceof NativeFrame) {
										selectorList = ((NativeFrame) rawFrame).getQueryStruct().getSelectors();
										if (selectorList.get(i) instanceof QueryColumnSelector) {
											table = ((QueryColumnSelector) selectorList.get(i)).getTable();
										}
									} else if (table != null && table.startsWith("FRAME")) {
										// This is H2 or R so table should be
										table = "H2_or_R_Temp_Table";
									}
									// dont iterate if tinker because tinker doesnt have columns
									if (rawFrame instanceof TinkerFrame) {
										// Tinker frame so get database from myHash, and set column to nothing
										curExpression += "\"dbName\":\"" + database + "\",\"tableName\": \"" + table + "\",\"columnName\": \"" + "Tinker_No_Columns" + "\",\"columnType\":\"" + columnType + "\",\"component\": \"" + key + "\",\"numUniqueValues\":20,\"entropy\":20}";
										break;
									} else {
										curExpression += "\"dbName\":\"" + database + "\",\"tableName\": \"" + table + "\",\"columnName\": \"" + column + "\",\"columnType\":\"" + columnType + "\",\"component\": \"" + key + "\",\"numUniqueValues\":20,\"entropy\":20}";
										entryNum += 1;
										if (!((i + 1) == value2.size())) {
											curExpression += ",{";
										}
									}
								}
							}
						}
					} catch (Exception e) {

					}
				}
			}
			curExpression += "]}}";
			return curExpression;
		}
	}

	public HashMap<String, String> createHash(Map<String, Object> meta) {
		HashMap<String, String> myHash = new HashMap<String, String>();
		ArrayList headersQs = (ArrayList) meta.get("headers");
		Iterator iter = headersQs.iterator();
		while (iter.hasNext()) {
			try {
				HashMap pair = (HashMap) iter.next();
				HashMap db_column = (HashMap) pair.get("qsName");
				String firstKey = (String) db_column.keySet().iterator().next();
				String value = db_column.get(firstKey) + "";
				value = (value.replaceAll("\\[", "")).replaceAll("\\]", "");
				if (value.contains("__")) {
					String[] array = value.split("__");
					value = array[1];
				}
				myHash.put(value, firstKey);
			} catch (Exception e) {
			}
		}
		return myHash;
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
			in.trackPixels("dataquery:", exprStart + exprBuilder.toString() + exprEnd);
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
	 * @param databaseName
	 * @param file
	 * @param headersAndTypes
	 */
	public static void trackExcelUpload(String databaseName, String file, Object headersAndTypes) {
		
		
	}
	
	/**
	 * Track a csv upload into a database
	 * @param databaseName
	 * @param file
	 * @param headersAndTypes
	 */
	public static void trackCsvUpload(String databaseName, String file, Object headersAndTypes) {

	}
}
