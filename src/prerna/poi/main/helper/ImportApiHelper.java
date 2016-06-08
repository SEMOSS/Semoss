package prerna.poi.main.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class ImportApiHelper {

	private Map<String,String> dataTypeMap = new HashMap<String, String>();
	private Map<Integer, HashMap<String, String>> allDataMap = new HashMap<Integer, HashMap<String, String>>();
	private HashMap<String, String> dataMap;

	private String[] allHeaders = null;
	private String[] allDatatypes = null;

	private static int rowCounter = 0;

	private Map<String, Object> extractObject;

	private String api = "";

	public void setApi(String api){
		this.api = api;
	}

	/**
	 * Use import.io api to extract web data
	 *@throws IOException
	 */
	private void extractData() throws IOException{

		URL importioApi = new URL(api);
		URLConnection urlConnection =  importioApi.openConnection();		
		urlConnection.connect();

		Gson gson = new Gson();
		InputStream in = null;
		try{
			in = urlConnection.getInputStream();
		}catch(Exception e){
			System.out.println("Error in extracting data from api: "+e);
		}
		JsonReader reader = new JsonReader(new InputStreamReader(in));
		JsonObject rootObj = (JsonObject) (new JsonParser()).parse(reader);		

		extractObject = gson.fromJson(rootObj.get("extractorData").getAsJsonObject(), new TypeToken<Map<String, Object>>() {}.getType());	

	}

	/*Parse JSON extracted data  
	 * to get all headers, datatypes for corresponding headers,
	 * and get data row by row to create H2TinkerFrame
	 * JSON Structure is expected to remain same:
	 * */
	public void parse() throws IOException{
		extractData();
		rowCounter = 0;
		Integer i = 0;
		List<Map<String, List<Map<String, List<Map<String, String>>>>>> dataList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) extractObject.get("data");
		for(Map<String, List<Map<String, List<Map<String, String>>>>> dataElement : dataList){//gets group as object here
			for(Map.Entry<String, List<Map<String, List<Map<String, String>>>>> groupEntry : dataElement.entrySet()) {
				if(groupEntry.getKey().equals("group")){
					List<Map<String, List<Map<String, String>>>> groupList = groupEntry.getValue();
					for(Map<String, List<Map<String, String>>> rowMap : groupList){

						dataMap = new HashMap<String,String>();
						for(Map.Entry<String, List<Map<String, String>>> rowEntry : rowMap.entrySet()){//<header, <datatype, data>>

							String header = rowEntry.getKey();
							List<Map<String, String>> cellList = rowEntry.getValue();
							String cellData = "";
							for(Map<String, String> cellMap : cellList){								
								for(Map.Entry<String, String> cellEntry : cellMap.entrySet()){
									if(!cellEntry.getKey().equalsIgnoreCase("href") && !cellEntry.getKey().equalsIgnoreCase("src")){
										String datatype = (String)cellEntry.getKey();
										cellData += ((String) cellEntry.getValue()).replaceAll("\n", "");
										cellData = cellData.replaceAll("[#%!&()@#$'/-]*", "");
										cellData = cellData.replaceAll("\\s+","_");
										cellData = cellData.replaceAll(",",""); 
										cellData = cellData.replaceAll("\"","_");
										cellData = cellData.replaceAll("__","_");
										dataMap.put(header, cellData);

										if(!dataTypeMap.keySet().contains(header)){											
											dataTypeMap.put(header, datatype);
										}
									}
								}
							}								
						}
						allDataMap.put(i, dataMap);
						i++;

					}
				}
			}
		}
	}

	//return String[] for list of headers in imported data to create insert query for H2DataFrame
	public String[] getHeaders(){
		Object[] headerObj = dataTypeMap.keySet().toArray();
		allHeaders = Arrays.copyOf(headerObj, headerObj.length, String[].class);
		return allHeaders;

	}

	//return String[] for each row to create insert query for H2DataFrame
	public String[] getNextRow(){

		for (int count : allDataMap.keySet()) {
			if(rowCounter == count){
				dataMap = allDataMap.get(count);

				List<String> dataRow = new ArrayList<String>();

				for(String header : dataTypeMap.keySet()){
					if(dataMap.get(header) == null)
						dataRow.add("");
					else
						dataRow.add(dataMap.get(header));
				}
				String[] nextRow = Arrays.copyOf(dataRow.toArray(), dataRow.toArray().length, String[].class);
				rowCounter++;
				return nextRow;
			}else
				continue;
		}
		return null;
	}

	public String[] getDatatypes(){
		List<String> types = new ArrayList<String>();
		for(Object key : dataTypeMap.keySet()){
			types.add(dataTypeMap.get(key));
		}
		Object[] datatypeObj = types.toArray();
		allDatatypes = Arrays.copyOf(datatypeObj, datatypeObj.length, String[].class);
		return allDatatypes;
	}

	public int getDataSize(){
		return allDataMap.keySet().size();
	}

	/*
	 * 11 data types in import.io:
	 * Text, Date/Time, Currency, Whole number, Number,
	 * Language, Country, Boolean, Link, Image, HTML
	 * --Free account gets only text datatype
	 * Currently ignore - Link, Image and HTML
	 */
	public String[] predictTypes() {
		String[] types = getDatatypes();
		String[] rawTypes = types;
		int counter = 0;
		for(String type : rawTypes){
			if(type.equalsIgnoreCase("Text") || type.equalsIgnoreCase("Language") || type.equalsIgnoreCase("Country")){
				type = "VARCHAR(800)";
			}else if(type.toUpperCase().contains("Number")){
				type = "DOUBLE";
			}else if(type.equalsIgnoreCase("Boolean")){
				type = "BOOLEAN";
			}else if(type.toUpperCase().contains("DATE")){//confirm if date is currently supported, I know its not at the FE, no way to get chronological order
				type = "DATE";
			}
			types[counter] = type;
			counter++;
		}
		return types;
	}

	//For testing
	public static void main(String[] args) throws IOException{

		ImportApiHelper helper = new ImportApiHelper();
		//helper.setApi("https://extraction.import.io/query/extractor/5702cd8d-82d1-481c-a3b9-e1bac8ea7533?_apikey=ab733af8464e4aa597e01e5f96fd60a8820cf0ebcb07fd24106ba9cd8f2cfa1da647aa9f7a2df55079ef2a8fbd4d29623520f9d3edef779ce8ab5e59fc6b893cc6393d9276519b0ef7aa1be0fc5f2022&url=http%3A%2F%2Fwww.ikea.com%2Fus%2Fen%2Fsearch%2F%3Fquery%3Dtable");
		//helper.setApi("https://extraction.import.io/query/extractor/5702cd8d-82d1-481c-a3b9-e1bac8ea7533?_apikey=ab733af8464e4aa597e01e5f96fd60a8820cf0ebcb07fd24106ba9cd8f2cfa1da647aa9f7a2df55079ef2a8fbd4d29623520f9d3edef779ce8ab5e59fc6b893cc6393d9276519b0ef7aa1be0fc5f2022&url=http%3A%2F%2Fwww.ikea.com%2Fus%2Fen%2Fsearch%2F%3Fquery%3Dtable");
		helper.setApi("https://extraction.import.io/query/extractor/f2f7910c-274d-4111-9264-184477512188?_apikey=ab733af8464e4aa597e01e5f96fd60a8820cf0ebcb07fd24106ba9cd8f2cfa1da647aa9f7a2df55079ef2a8fbd4d29623520f9d3edef779ce8ab5e59fc6b893cc6393d9276519b0ef7aa1be0fc5f2022&url=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FDancing_with_the_Stars_(U.S._TV_series)");
		helper.extractData();
		helper.parse();
		System.out.println("**");

		String[] headers = helper.getHeaders();
		System.out.println("***Headers:");
		for(String header: headers){
			System.out.print(header + "|");
		}

		String[] datatypes = helper.getDatatypes();
		System.out.println("\n***Datatypes:");
		for(String datatype: datatypes){
			System.out.print(datatype + "|");
		}

		int i = 0;
		String[] rowData;
		System.out.println("No. of rows: " + helper.getDataSize());
		while(i<helper.getDataSize()){
			rowData = helper.getNextRow();
			if(rowData != null){

				System.out.println("\n***RowData " + i +" :");
				for(String cell: rowData){
					System.out.print(cell +"|");
				}
				System.out.println("***");
				i++;
			}
		}



	}

}

