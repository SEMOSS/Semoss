package prerna.sablecc2.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.twitter.TwitterSearcher;
import prerna.om.Viewpoint;

public class TwitterSearchLambda extends AbstractFlatMapLambda {

	// col index we care about to get lat/long from
	private int colIndex;
	// total number of columns
	private int totalCols;
	
	@Override
	public List<IHeadersDataRow> process(IHeadersDataRow row) {
		Object value = row.getValues()[colIndex];
		if(value == null || value.toString().isEmpty()) {
			return new Vector<IHeadersDataRow>();
		}
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[]{"review", "author", "retweet_count"};
		
		Hashtable params = new Hashtable();
		params.put("q", value.toString().replace("_", " "));
		params.put("lang", "en");
		if(this.params.containsKey("output"))
			params.put("count", this.params.get("output"));		
		else
			params.put("count", "10");
		
		if(this.params.containsKey("result_type"))
			params.put("result_type", this.params.get("result_type"));
		else
			params.put("result_type", "mixed");
		
		List<IHeadersDataRow> retList = new Vector<IHeadersDataRow>();
		// add new values
		try {
			// loop through the results
			TwitterSearcher ts = new TwitterSearcher();
			Object resultObj = ts.execute(this.user, params);
			if(resultObj instanceof List) {
				List<Viewpoint> results = (List<Viewpoint>) resultObj;
				for(int i = 0; i < results.size(); i++) {
					Viewpoint view = results.get(i);
					processView(view, newHeaders, row, retList);
				}
			} else {
				Viewpoint view = (Viewpoint) resultObj;
				processView(view, newHeaders, row, retList);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return retList;
	}
	
	/**
	 * Process a view point
	 * @param view
	 * @param newHeaders
	 * @param curRow
	 * @param retList
	 */
	private void processView(Viewpoint view, String[] newHeaders, IHeadersDataRow curRow, List<IHeadersDataRow> retList) {
		Object[] newValues = new Object[3];
		newValues[0] = view.getReview();
		if(newValues[0] != null) {
			newValues[0] = newValues[0].toString()
					.replace("\n", " *LINE BREAK* ")
					.replace("\r", " *LINE BREAK* ")
					.replace("\t", " ")
					.replace("\"", "");
		}
		newValues[1] = view.getAuthorId();
		newValues[2] = view.getRepeatCount();
		
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = curRow.copy();
		rowCopy.addFields(newHeaders, newValues);
		retList.add(rowCopy);
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
