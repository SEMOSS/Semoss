package prerna.engine.api;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import prerna.ds.util.WebApiIterator;

public class WebApi implements IApi{
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	
	//String [] params = {"API_PARAM"};
	String [] params = {"URL", "ITEM_SEARCH", "ITEM_LOOKUP"};

	@Override
	public String[] getParams() {
		// TODO Auto-generated method stub
		return params;
	}

	@Override
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		values.put(key, value);
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		
		String url = (String) values.get("URL");
		String itemSearch = (String) values.get("ITEM_SEARCH");
		String itemLookup = (String) values.get("ITEM_LOOKUP");
		String key = (url != null)? "URL" : (itemSearch != null)? "ITEM_SEARCH" : "ITEM_LOOKUP";
		
		Iterator<IHeadersDataRow> it;
		try {
			/*if(keyvalue.matches("^(https?|ftp)://.*$"))
				it = new ImportApiIterator(keyvalue, null);
			else
				it = new ImportApiIterator(keyvalue);*/
			
			switch(key){
			case "URL":	it = new WebApiIterator(url); break;
			case "ITEM_SEARCH":	it = new WebApiIterator("ItemSearch", itemSearch); break;
			case "ITEM_LOOKUP": it = new WebApiIterator("ItemLookup", itemLookup); break;
			default: it = null; break;
			}
			

			return it;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		return null;
	}
}
