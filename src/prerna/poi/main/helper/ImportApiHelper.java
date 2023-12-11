package prerna.poi.main.helper;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;

public class ImportApiHelper extends WebAPIHelper{

	private Map<String, Object> extractObject;	

	
	public void parse(){
		parseData();
	}
	
	/**
	 * Use import.io api to extract web data
	 */
	private void extractData(){

		URL importioApi;
		URLConnection urlConnection;
		InputStream in;
		Gson gson;
		try {
			importioApi = new URL(api);
			urlConnection =  importioApi.openConnection();		
			urlConnection.connect();
			gson = new Gson();
			
			in = urlConnection.getInputStream();
			JsonReader reader = new JsonReader(new InputStreamReader(in));
			JsonObject rootObj = (JsonObject) (new JsonParser()).parse(reader);	
			
			extractObject = gson.fromJson(rootObj.get("extractorData").getAsJsonObject(), new TypeToken<Map<String, Object>>() {}.getType());	

			
		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	/*Parse JSON extracted data  
	 * to get all headers, datatypes for corresponding headers,
	 * and get data row by row to create H2Frame
	 * JSON Structure is expected to remain same:
	 * */
	private void parseData(){
		extractData();
		setRowCounter(0);
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

	

	//For testing
//	public static void main(String[] args) throws IOException{
//
//		ImportApiHelper helper = new ImportApiHelper();
//		helper.setApiParam("https://extraction.import.io/query/extractor/f2f7910c-274d-4111-9264-184477512188?_apikey=ab733af8464e4aa597e01e5f96fd60a8820cf0ebcb07fd24106ba9cd8f2cfa1da647aa9f7a2df55079ef2a8fbd4d29623520f9d3edef779ce8ab5e59fc6b893cc6393d9276519b0ef7aa1be0fc5f2022&url=http%3A%2F%2Fen.wikipedia.org%2Fwiki%2FDancing_with_the_Stars_(U.S._TV_series)");
//		helper.parse();
//		System.out.println("**");
//
//		String[] headers = helper.getHeaders();
//		System.out.println("***Headers:");
//		for(String header: headers){
//			System.out.print(header + "|");
//		}
//
//		String[] datatypes = helper.getDatatypes();
//		System.out.println("\n***Datatypes:");
//		for(String datatype: datatypes){
//			System.out.print(datatype + "|");
//		}
//
//		int i = 0;
//		String[] rowData;
//		System.out.println("No. of rows: " + helper.getDataSize());
//		while(i<helper.getDataSize()){
//			rowData = helper.getNextRow();
//			if(rowData != null){
//
//				System.out.println("\n***RowData " + i +" :");
//				for(String cell: rowData){
//					System.out.print(cell +"|");
//				}
//				System.out.println("***");
//				i++;
//			}
//		}
//
//
//
//	}

}

