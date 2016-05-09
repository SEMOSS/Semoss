package prerna.engine.impl.rdf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import prerna.cache.FileStore;
import prerna.ds.QueryStruct;
import prerna.ds.util.FileIterator;
import prerna.engine.api.IApi;
import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.rdf.query.builder.IQueryInterpreter;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class QueryAPI implements IApi {
	
	Hashtable <String, Object> values = new Hashtable<String, Object>();
	String [] params = {"QUERY_STRUCT", "ENGINE"};
	
	@Override
	public void set(String key, Object value) {
		values.put(key, value);
	}

	@Override
	public Iterator process() {
		// get basic data
		QueryStruct qs = (QueryStruct) values.get(params[0]); 
//		loadEngine4Test();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((values.get(params[1]) + "").trim()); 
		if(engine != null){
//			loadEngine4Test();
//			engine = (IEngine) DIHelper.getInstance().getLocalProp((values.get(params[1]) + "").trim());
//			if(engine != null) {
			qs.print();
			IQueryInterpreter interp = engine.getQueryInterpreter();
			interp.setQueryStruct(qs);
			String query = interp.composeQuery();
			
			// play
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			
			// give back
			return wrapper;
			
		} 
		else {
			Hashtable<String, Vector<String>> selectorSet = qs.getSelectors();
			String fileName = values.get(params[1]) + "";
			
			// look to see if this file is actually a unique id for a file that is on the server
			if(FileStore.getInstance().containsKey(fileName)) {
				fileName = FileStore.getInstance().get(fileName);
			}
			
			return new FileIterator(fileName, ',', qs, null); //TODO: right now making the assumption it is a csv file
//			return null;
		}
		
		//return null;
	}

	private void loadEngine4Test(){
		File f = new File("C:\\Users\\bisutton\\workspace\\SEMOSSDev\\RDF_Map.prop");
		if(f.exists() && !f.isDirectory()) { 
			DIHelper.getInstance().loadCoreProp("C:\\Users\\bisutton\\workspace\\SEMOSSDev\\RDF_Map.prop");
			FileInputStream fileIn = null;
			try{
				Properties prop = new Properties();
				String fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\UpdatedRDBMSMovies.smss";
				fileIn = new FileInputStream(fileName);
				prop.load(fileIn);
				System.err.println("Loading DB " + fileName);
				Utility.loadEngine(fileName, prop);
				fileName = "C:\\Users\\bisutton\\workspace\\SEMOSSDev\\db\\Movie_DB.smss";
				fileIn = new FileInputStream(fileName);
				prop.load(fileIn);
				System.err.println("Loading DB " + fileName);
				Utility.loadEngine(fileName, prop);
			}catch(IOException e){
				e.printStackTrace();
			}finally{
				try{
					if(fileIn!=null)
						fileIn.close();
				}catch(IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public String[] getParams() {
		return params;
	}
}
