package prerna.poi.main.helper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

public class AmazonApiHelper extends WebAPIHelper{

	//amazon API parameters
	private final static String ENDPOINT = "webservices.amazon.com";
	private final static String AWS_ACCESS_KEY_ID = "***REMOVED***";
	private final static String AWS_SECRET_KEY = "da+LAFM8SS+Bsrs0UPX8vaqyjL4E39FLLycxfL0Q";
	private final static String ASSOCIATE_TAG = "semoss-20";
	private final static int PAGE_THRESHOLD = 5;

	private Map<String, String> requestParams = new HashMap<String, String>();

	private XMLStreamReader reader = null;

	private static int totalPages = 1;
	private static int currentPage = 1;
	private static String operationType = "ItemSearch";

	public void setApiParam(String item){
		switch(requestParams.get("Operation")){
		case "ItemSearch":
			requestParams.put("Keywords", item); 
			operationType = "ItemSearch"; break;
		case "ItemLookup":
			requestParams.put("ItemId", item); 
			operationType = "ItemLookup"; break;
		}
	}

	//set operation type here - ItemSearch or ItemLookup
	public void setOperationType(String operation){
		requestParams.put("Operation", operation);
	}

	private String getApi(){

		SignedRequestsHelper requestHelper;
		try {
			requestHelper = SignedRequestsHelper.getInstance(ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_KEY);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		this.api = requestHelper.sign(requestParams);
		return this.api;
	}

	//set all basic required API request parameters
	private void setRequestParams(){

		switch(requestParams.get("Operation")){
		case "ItemSearch":
			requestParams.put("Service", "AWSECommerceService");
			requestParams.put("AWSAccessKeyId", AWS_ACCESS_KEY_ID);
			requestParams.put("AssociateTag", ASSOCIATE_TAG);
			requestParams.put("SearchIndex", "All");//search within all categories
			requestParams.put("ResponseGroup", "Small,OfferSummary,Similarities");//Small will give the properties of the item searched, OfferSummary includes the price
			requestParams.put("Availability", "Available");//filter out most of the items that are unavailable
			break;

		case "ItemLookup":
			requestParams.put("Service", "AWSECommerceService");
			requestParams.put("AWSAccessKeyId", AWS_ACCESS_KEY_ID);
			requestParams.put("AssociateTag", ASSOCIATE_TAG);
			requestParams.put("IdType", "ASIN");
			requestParams.put("ResponseGroup", "ItemAttributes,OfferSummary");//ItemAttributes will give the properties of the item searched, OfferSummary includes the price

		}

	}

	private void addRequestParam(String paramKey, String paramValue){
		requestParams.put(paramKey, paramValue);
		//eg: paramKey: ItemPage
	}

	public void parse(){
		setRequestParams();
		switch(requestParams.get("Operation")){
		case "ItemSearch":
			extractSearchData(); break;
		case "ItemLookup":
			extractLookupData(); break;
		}
		currentPage = 1;
		setRowCounter(0);

	}

	/**
	 * Use Amazon Product Advertising API to extract Item Search data
	 * Response is available as an XML document
	 */
	private void extractSearchData(){
		//setRequestParams();
		while(currentPage <= totalPages && currentPage <= PAGE_THRESHOLD){

			if(totalPages > 1){
				int nextPage = currentPage;
				addRequestParam("ItemPage", String.valueOf(nextPage));
			}	
			extractLookupData();
			/*URL amazonApi;
			try {
				amazonApi = new URL(getApi());
				System.out.println(currentPage + ". Amazon API: " +amazonApi);
				XMLInputFactory factory = XMLInputFactory.newInstance();
				reader = factory.createXMLStreamReader(amazonApi.openStream());
				parseData();

			} catch (MalformedURLException e1) {
				e1.printStackTrace();
			}catch (XMLStreamException e) {				
				System.out.println("Error in extracting data from api: "+e);
			} catch (IOException e) {
				e.printStackTrace();
			}*/					
			currentPage++;
		}	
		/*currentPage = 1;
		setRowCounter(0);*/
	}

	/**
	 * Use Amazon Product Advertising API to extract Item Lookup data
	 * Response is available as an XML document
	 */
	private void extractLookupData(){

		URL amazonApi;
		try {
			amazonApi = new URL(getApi());
			System.out.println(currentPage + ". Amazon API: " +amazonApi);
			XMLInputFactory factory = XMLInputFactory.newInstance();
			// to be compliant, completely disable DOCTYPE declaration:
			factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
			// or completely disable external entities declarations:
			factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
			// or prohibit the use of all protocols by external entities:
			factory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
			factory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			reader = factory.createXMLStreamReader(amazonApi.openStream());
			parseData();

		} catch (MalformedURLException e1) {
			e1.printStackTrace();
		}catch (XMLStreamException e) {				
			System.out.println("Error in extracting data from api: "+e);
		} catch (IOException e) {
			e.printStackTrace();
		}			
	}

	private void parseData(){
		String name = "",value = "", mainTag = "", header = "", datatype = "", errorCode = "", errorMessage = "";
		int totalResults = 0, similarProductCount = 0, actorCount = 0;
		boolean isSuccess = false;
		try {
			while(reader.hasNext()) {

				int e = (int) reader.next();					

				switch(e){
				case XMLStreamReader.START_ELEMENT:
					name = ((XMLStreamReader) reader).getLocalName().trim();

					switch (name){
					case "Item":
						dataMap = new HashMap<String, String>();
						similarProductCount = 0; 
						actorCount = 0;
						mainTag = ""; break;

					case "LowestNewPrice":
					case "LowestUsedPrice":
					case "LowestRefurbishedPrice":
					case "Error":	mainTag = name; break;
					case "SimilarProduct":	mainTag = name;
						similarProductCount += 1; break;
					case "Actor": mainTag = name;
						actorCount += 1; break;

					}
					break;

				case XMLStreamReader.CHARACTERS:
					value = ((XMLStreamReader) reader).getText().trim();
					switch (name) {
					case "Code":
						if(mainTag.equals("Error")) 
							errorCode = value; break;

					case "Message":
						if(mainTag.equals("Error"))
							errorMessage = value; break;

					case "TotalPages": totalPages = Integer.valueOf(value); break;

					case "TotalResults":	totalResults = Integer.valueOf(value);
					if (totalResults > 0)
						isSuccess = true;
					break;						

					case "ASIN":

					case "Title":
						if(mainTag.equals("SimilarProduct")){
							header = mainTag + "_" + similarProductCount + "_" + name;
						}
						else
							header = "Item" +"_" + name;
						//dataMap.put(header, value);
						dataMap.put(header, (value == null)? "" : value);
						datatype = "Text";
						break;

					case "FormattedPrice":
						if(mainTag.equals("LowestNewPrice") || mainTag.equals("LowestUsedPrice")
								|| mainTag.equals("LowestRefurbishedPrice")) {
							header = mainTag;
							mainTag = "";
							if(value.equalsIgnoreCase("TOO LOW TO DISPLAY") || value == null){
								value = "0.00";								
							}
							value = value.replace("$", "");
							dataMap.put(header, value);
							datatype = "Number";
						}							
						break;

					case "ProductTypeName": header = "ProductType"; 
						//dataMap.put(header, value);
						dataMap.put(header, (value == null)? "" : value);
						datatype = "Text"; break;

					case "Actor": 
						if(operationType.endsWith("ItemSearch"))
							continue;
						if(mainTag.equals("Actor")){
							header = mainTag + "_" + actorCount;
						}
						//dataMap.put(header, value);
						dataMap.put(header, (value == null)? "" : value);
						datatype = "Text"; break;

					case "AudienceRating":
					case "Author":
					case "Brand":
					case "Category":
					case "Director":
					case "Genre":					
					case "PublicationDate":
					case "Publisher":
					case "ReleaseDate":
					case "Studio":
						if(operationType.endsWith("ItemSearch"))
							continue;
						
					case "Manufacturer":
					case "ProductGroup":
						header = name;
						//dataMap.put(header, value);
						dataMap.put(header, (value == null)? "" : value);
						datatype = "Text"; break;

					}
					break;

				case XMLStreamReader.END_ELEMENT:
					name = ((XMLStreamReader) reader).getLocalName().trim();
					if(name.equals("Item")){
						if(dataMap != null){
							addItem(dataMap);							
						}
					}else if(name.equals("ItemSearchResponse") && currentPage > PAGE_THRESHOLD)	{
						if(getRowCounter() > 0)
							setRowCounter(0);
					}
					break;
				}

				if(!dataTypeMap.keySet().contains(header) && header != ""){											
					dataTypeMap.put(header, datatype);
				}				

				if(!isSuccess && errorCode != "" && errorMessage != ""){
					System.out.println("Error: "+errorCode +" : " + errorMessage);
					return;
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
	}


	private void addItem(Map<String,String> data){
		allDataMap.put(getRowCounter(), dataMap);
		setRowCounter(getRowCounter()+1);
		dataMap = null;
	}



//	//For testing
//	public static void main(String[] args) throws IOException{		
//
//		AmazonApiHelper helper = new AmazonApiHelper();
//		//helper.setOperationType("ItemSearch");		
//		//helper.setApiParam("iphone");//pass the keyword for ITemSearch
//		helper.setOperationType("ItemLookup");
//		helper.setApiParam("B01723YVFM");
//		//helper.extractData();
//		helper.parse();
//		System.out.println("allDataMap: "+helper.allDataMap);
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
//		System.out.println("\nNo. of rows: " + helper.getDataSize());
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
//	}

}
