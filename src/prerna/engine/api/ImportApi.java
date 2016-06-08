package prerna.engine.api;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import prerna.ds.util.ImportApiIterator;

public class ImportApi implements IApi{
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	String [] params = {"URL_MAP"};

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
		String url = (String) values.get(params[0]);
		
		Iterator<IHeadersDataRow> it;
		try {
			it = new ImportApiIterator(url, null);

			return it;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
		return null;
	}
}
