package prerna.sablecc;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import prerna.ds.util.WebApiIterator;
import prerna.engine.api.IHeadersDataRow;

public class WebApiReactor extends AbstractReactor{

	public final static String URL = "url";
	public final static String ITEM_SEARCH = "itemSearch";
	public final static String ITEM_LOOKUP = "itemLookup";
	
	private Map<String, String> mapOptions = null;
	
	private String url = null;
	private String itemSearch = null;
	private String itemLookup = null;	

	@Override
	public Iterator process() {

		String engine = (String)myStore.get("ENGINE");
		Iterator<IHeadersDataRow> it = null;

		try {
			if(engine.equalsIgnoreCase("ImportIO") || engine.equalsIgnoreCase("AmazonProduct")) {
				if(myStore.get(PKQLEnum.MAP_OBJ) != null){
					mapOptions = (Map<String, String>) myStore.get(PKQLEnum.MAP_OBJ);
					for(String key : mapOptions.keySet()) {
						switch(key){
						case URL: url = mapOptions.get(URL); 
								  it = new WebApiIterator(url); break;
						case ITEM_SEARCH: itemSearch = mapOptions.get(ITEM_SEARCH); 
										  it = new WebApiIterator("ItemSearch", itemSearch); break;
						case ITEM_LOOKUP: itemLookup = mapOptions.get(ITEM_LOOKUP); 
										  it = new WebApiIterator("ItemLookup", itemLookup); break;
						}				
					}
					
					this.put((String) getValue(PKQLEnum.API), it);
					this.put("RESPONSE", "success");
					this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
					return null;
				}else{
					String response = engine.equalsIgnoreCase("ImportIO") ? "Error: Invalid PKQL: Missing URL. Required Syntax: data.import(api:ImportIO.Query({'url':'enter_your_url_here'}));" : (engine.equalsIgnoreCase("AmazonProduct") ? "Error: Invalid PKQL: Required Syntax: data.import(api:AmazonProduct.Query({'itemSearch':'enter_search_keywords_here'})); OR data.import(api:AmazonProduct.Query({'itemLookup':'enter_item_ASIN_here'}));" : "Error: Invalid PKQL" );
					myStore.put("RESPONSE", response);
					myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
					return null;
				}
				
			}
		} catch (IOException e) {		
			e.printStackTrace();			
		}			

		return null;
	}
}
