package prerna.sablecc2.reactor.task.lambda.map;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleLatLongGetter;
import prerna.om.GeoLocation;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleLatLongLambda extends AbstractMapLambda {

	// col index we care about to get lat/long from
	private int colIndex;
	// total number of columns
	private int totalCols;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		// grab the column index we want to use as the address
		Hashtable params = new Hashtable();
		params.put("address", row.getValues()[colIndex]);
		
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[]{"lat", "long"};
		Object[] newValues = new Object[2];
		// add new values
		try {
			GoogleLatLongGetter goog = new GoogleLatLongGetter();
			// geo location object flushes the JSON return into something for getters and setters
			GeoLocation location = (GeoLocation) goog.execute(this.user, params);
			newValues[0] = location.getLatitude();
			newValues[1] = location.getLongitude();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		row.addFields(newHeaders, newValues);
		return row;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		if(this.user == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires login to google", PixelDataType.CONST_STRING));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		AccessToken googleAccess = this.user.getAccessToken(AuthProvider.GOOGLE.name());
		if(googleAccess == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires login to google", PixelDataType.CONST_STRING));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
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
