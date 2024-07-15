//package prerna.poi.main.helper;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.List;
//import java.util.Map;
//
//public class WebAPIHelper {
//
//	protected Map<String,String> dataTypeMap = new LinkedHashMap<String, String>();
//	protected static Map<Integer, HashMap<String, String>> allDataMap = new HashMap<Integer, HashMap<String, String>>();
//	protected HashMap<String, String> dataMap;
//
//	private String[] allHeaders = null;
//	private String[] allDatatypes = null;
//
//	private static int rowCounter = 0;
//
//	protected String api = "";
//
//	public void setApiParam(String api){
//		this.api = api;
//	}
//
//	public static int getRowCounter() {
//		return rowCounter;
//	}
//
//	public static void setRowCounter(int rowCounter) {
//		WebAPIHelper.rowCounter = rowCounter;
//	}
//
//	public void parse(){};
//
//	//return String[] for list of headers in web extracted data to create insert query for H2Frame
//	public String[] getHeaders(){
//		Object[] headerObj = dataTypeMap.keySet().toArray();
//		allHeaders = Arrays.copyOf(headerObj, headerObj.length, String[].class);
//		return allHeaders;
//
//	}
//
//	//return String[] for each row to create insert query for H2Frame
//	public String[] getNextRow(){
//
//		for (int count : allDataMap.keySet()) {
//			if(rowCounter == count){
//				dataMap = allDataMap.get(count);
//
//				List<String> dataRow = new ArrayList<String>();
//
//				for(String header : dataTypeMap.keySet()){
//					if(dataMap.get(header) == null)
//						dataRow.add("");
//					else{
//						//testing
//						if(dataMap.get(header).equalsIgnoreCase("TOO LOW TO DISPLAY"))
//							System.out.println("WebAPIHelper.getNextRow() for row "+ rowCounter);
//						dataRow.add(dataMap.get(header));
//					}
//				}
//				String[] nextRow = Arrays.copyOf(dataRow.toArray(), dataRow.toArray().length, String[].class);
//				rowCounter++;
//				return nextRow;
//			}else
//				continue;
//		}
//		return null;
//	}
//
//	public String[] getDatatypes(){
//		List<String> types = new ArrayList<String>();
//		for(Object key : dataTypeMap.keySet()){
//			types.add(dataTypeMap.get(key));
//		}
//		Object[] datatypeObj = types.toArray();
//		allDatatypes = Arrays.copyOf(datatypeObj, datatypeObj.length, String[].class);
//		return allDatatypes;
//	}
//
//	public int getDataSize(){
//		return allDataMap.keySet().size();
//	}
//
//	/*
//	 * 11 data types in import.io:
//	 * Text, Date/Time, Currency, Whole number, Number,
//	 * Language, Country, Boolean, Link, Image, HTML
//	 * --Free account gets only text datatype
//	 * Currently ignore - Link, Image and HTML
//	 */
//	public String[] predictTypes() {
//		String[] types = getDatatypes();
//		String[] rawTypes = types;
//		int counter = 0;
//		for(String type : rawTypes){
//			if(type.equalsIgnoreCase("Text") || type.equalsIgnoreCase("Language") || type.equalsIgnoreCase("Country")){
//				type = "VARCHAR(800)";
//			}else if(type.toUpperCase().contains("Number")){
//				type = "DOUBLE";
//			}else if(type.equalsIgnoreCase("Boolean")){
//				type = "BOOLEAN";
//			}else if(type.toUpperCase().contains("DATE")){//confirm if date is currently supported, I know its not at the FE, no way to get chronological order
//				type = "DATE";
//			}
//			types[counter] = type;
//			counter++;
//		}
//		return types;
//	}
//
//	
//
//
//}
