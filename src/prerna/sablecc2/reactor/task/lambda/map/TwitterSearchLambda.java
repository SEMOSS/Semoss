package prerna.sablecc2.reactor.task.lambda.map;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.twitter.TwitterSearcher;
import prerna.om.HeadersDataRow;
import prerna.om.Viewpoint;

public class TwitterSearchLambda extends AbstractMapLambda {

	// col index we care about to get lat/long from
	private int colIndex;
	// total number of columns
	private int totalCols;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		String[] headers = row.getHeaders();
		Object[] values = row.getValues();
		
		String[] newHeaders = new String[this.totalCols+3];
		Object[] newValues = new Object[this.totalCols+3];
		
		System.arraycopy(headers, 0, newHeaders, 0, this.totalCols);
		System.arraycopy(values, 0, newValues, 0, this.totalCols);

		// add new headers
		newHeaders[this.totalCols] = "review";
		newHeaders[this.totalCols+1] = "author";
		newHeaders[this.totalCols+2] = "retweet_count";

		Hashtable params = new Hashtable();
		params.put("q", values[colIndex]);
		params.put("lang", "en");
		params.put("count", "1");

		// add new values
		try {
			TwitterSearcher ts = new TwitterSearcher();
			List<Viewpoint> results = (List<Viewpoint>) ts.execute(this.user, params);
			Viewpoint view = results.get(0);
			System.out.println(view);
			
			newValues[this.totalCols] = view.getReview();
			newValues[this.totalCols+1] = view.getAuthorId();
			newValues[this.totalCols+2] = view.getRepeatCount();

		} catch(Exception e) {
			e.printStackTrace();
		}
		return new HeadersDataRow(newHeaders, newValues);
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
		Map<String, Object> reviewHeader = getBaseHeader("review", "STRING");
		this.headerInfo.add(reviewHeader);
		
		Map<String, Object> authHeader = getBaseHeader("author", "STRING");
		this.headerInfo.add(authHeader);
		
		Map<String, Object> repeatHeader = getBaseHeader("retweet_count", "NUMBER");
		this.headerInfo.add(repeatHeader);
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
