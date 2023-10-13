package prerna.reactor.task.lambda.map.function;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleLatLongGetter;
import prerna.om.GeoLocation;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GoogleLatLongLambda extends AbstractMapLambda {

	// cahing of some results
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	private static String cache_file_loc = null;
	private static Map<String, List<Double>> localcache = new HashMap<String, List<Double>>();

	static {
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		cache_file_loc = baseFolder + DIR_SEPARATOR + "geo" + DIR_SEPARATOR + "latlong.json";
		File f = new File(cache_file_loc);
		if(f.exists()) {
			Map<String, List<Double>> mapData = null;
			try {
				mapData = new ObjectMapper().readValue(f, Map.class);
			} catch (IOException e) {
				e.printStackTrace();
				// do noting
			}
			
			if(mapData != null) {
				localcache.putAll(mapData);
			}
		}
	}
	
	// col index we care about to get lat/long from
	private int colIndex;
	// total number of columns
	private int totalCols;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[]{"lat", "long"};
		Object[] newValues = new Object[2];
					
		// grab the column index we want to use as the address
		String address = row.getValues()[colIndex].toString().toLowerCase().replace("_", " ");
		if(localcache.containsKey(address)) {
			List<Double> cacheValues = localcache.get(address);
			newValues[0] = cacheValues.get(0); 
			newValues[1] = cacheValues.get(1);
		} else {
			Hashtable params = new Hashtable();
			params.put("address", address);
			
			// add new values
			try {
				GoogleLatLongGetter goog = new GoogleLatLongGetter();
				// geo location object flushes the JSON return into something for getters and setters
				GeoLocation location = (GeoLocation) goog.execute(this.user, params);
				newValues[0] = location.getLatitude();
				newValues[1] = location.getLongitude();
				
				// cache it
				List<Double> cacheV = new Vector<Double>();
				cacheV.add((double) location.getLatitude());
				cacheV.add((double) location.getLongitude());
				localcache.put(address, cacheV);
				
//				File f = new File(cache_file_loc);
//				if(!f.getParentFile().exists()) {
//					f.getParentFile().mkdirs();
//				}
//				if(f.exists()) {
//					f.delete();
//				}
//				try {
//					Gson gson = new GsonBuilder().setPrettyPrinting().create();
//					// write json to file
//					FileUtils.writeStringToFile(f, gson.toJson(localcache));
//				} catch (IOException e1) {
//					e1.printStackTrace();
//				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		row.addFields(newHeaders, newValues);
		return row;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		this.totalCols = headerInfo.size();
		
		String headerToConvert = columns.get(0);
		for(int j = 0; j < totalCols; j++) {
			Map<String, Object> headerMap = headerInfo.get(j);
			String alias = headerMap.get("alias").toString();
			if(alias.equals(headerToConvert)) {
				// we found the index
				this.colIndex = j;
			}
		}
		
		// this modifies the header info map by reference
		Map<String, Object> latHeader = getBaseHeader("lat", "NUMBER");
		this.headerInfo.add(latHeader);
		Map<String, Object> longHeader = getBaseHeader("long", "NUMBER");
		this.headerInfo.add(longHeader);
	}
	
	/**
	 * Grab a base header object
	 * @param name
	 * @param type
	 * @return
	 */
	private Map<String, Object> getBaseHeader(String name, String type) {
		Map<String, Object> header = new HashMap<String, Object>();
		header.put("alias", name);
		header.put("header", name);
		header.put("derived", true);
		header.put("type", type.toUpperCase());
		return header;
	}

}
