package prerna.engine.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.ds.util.WebApiIterator;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner;

public class WebApi extends AbstractReactor{
	
	private String url = null;
	private String itemSearch = null;
	private String itemLookup = null;
	
	@Override
	public Iterator process() {
		// grab the engine from the my store
		String engine = (String)myStore.get("ENGINE");
		
		// we currently have 2 different cases for the web api
		if(engine.equalsIgnoreCase("ImportIO")){
			if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
				if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("url")){
					this.url = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("url");
				} else {
					System.out.println("Invalid PKQL: Missing URL. Required Syntax: data.import(api:ImportIO.Query({'url':'enter_your_url_here'})); ");
					myStore.put("RESPONSE", "Error: Invalid PKQL: Missing URL. Required Syntax: data.import(api:ImportIO.Query({'url':'enter_your_url_here'}));");
					myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
					return null;
				}
			}
		} else if(engine.equalsIgnoreCase("AmazonProduct")){
			if(myStore.get(PKQLEnum.G) instanceof H2Frame) {
				if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("itemSearch")){
					this.itemSearch = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("itemSearch");
				} else if(myStore.containsKey("KEY_VALUE") && ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).containsKey("itemLookup")){
					this.itemLookup = (String) ((Map) ((Vector) myStore.get("KEY_VALUE")).get(0)).get("itemLookup");
				} else {
					System.out.println("Invalid PKQL: Required Syntax: data.import(api:AmazonProduct.Query({'itemSearch':'enter_search_keywords_here'})); OR data.import(api:AmazonProduct.Query({'itemLookup':'enter_item_ASIN_here'}));");
					myStore.put("RESPONSE", "Error: Invalid PKQL: Required Syntax: data.import(api:AmazonProduct.Query({'itemSearch':'enter_search_keywords_here'})); OR data.import(api:AmazonProduct.Query({'itemLookup':'enter_item_ASIN_here'}));");
					myStore.put("STATUS", PKQLRunner.STATUS.ERROR);
					return null;
				}
			}
		}
		
		String key = (this.url != null)? "URL" : (this.itemSearch != null)? "ITEM_SEARCH" : "ITEM_LOOKUP";
		
		Iterator<IHeadersDataRow> it = null;
		try {
			switch(key){
			case "URL":	it = new WebApiIterator(url); break;
			case "ITEM_SEARCH":	it = new WebApiIterator("ItemSearch", itemSearch); break;
			case "ITEM_LOOKUP": it = new WebApiIterator("ItemLookup", itemLookup); break;
			default: it = null; break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
		
		this.put((String) getValue(PKQLEnum.API), it);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}
}
