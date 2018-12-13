package prerna.sablecc2.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleEntityResolver;
import prerna.om.EntityResolution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleEntityAnalyzerLambda extends AbstractFlatMapLambda {

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
		// grab the column index we want to use as the address
		Hashtable params = new Hashtable();
		Hashtable docParam = new Hashtable();
		docParam.put("type", "PLAIN_TEXT");
		docParam.put("language", "EN");
		docParam.put("content", value.toString().replace("_", " "));
		params.put("document", docParam);
		
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[]{"entity_name", "entity_type", "wiki_url", "content", "content_subtype"};
		
		List<IHeadersDataRow> retList = new Vector<IHeadersDataRow>();
		try {
			// loop through the results
			GoogleEntityResolver goog = new GoogleEntityResolver();
			Object resultObj = goog.execute(this.user, params);
			if(resultObj instanceof List) {
				List<EntityResolution> results = (List<EntityResolution>) resultObj;
				for(int i = 0; i < results.size(); i++) {
					EntityResolution entity = results.get(i);
					processEntity(entity, newHeaders, row, retList);
				}
			} else {
				EntityResolution entity = (EntityResolution) resultObj;
				processEntity(entity, newHeaders, row, retList);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return retList;
	}
	
	private void processEntity(EntityResolution entity, String[] newHeaders, IHeadersDataRow curRow, List<IHeadersDataRow> retList) {
		Object[] newValues = new Object[5];
		newValues[0] = entity.getEntity_name();
		newValues[1] = entity.getEntity_type();
		newValues[2] = entity.getWiki_url();
		newValues[3] = entity.getContent();
		newValues[4] = entity.getContent_subtype();

		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = curRow.copy();
		rowCopy.addFields(newHeaders, newValues);
		retList.add(rowCopy);
	}
	
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		if(this.user == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires login to google", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		AccessToken googleAccess = this.user.getAccessToken(AuthProvider.GOOGLE);
		if(googleAccess == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires login to google", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
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
		Map<String, Object> entityHeader = getBaseHeader("entity_name", "STRING");
		this.headerInfo.add(entityHeader);
		Map<String, Object> typeHeader = getBaseHeader("entity_type", "STRING");
		this.headerInfo.add(typeHeader);
		Map<String, Object> wikiHeader = getBaseHeader("wiki_url", "STRING");
		this.headerInfo.add(wikiHeader);
		Map<String, Object> contentHeader = getBaseHeader("content", "STRING");
		this.headerInfo.add(contentHeader);
		Map<String, Object> contentTypeHeader = getBaseHeader("content_subtype", "STRING");
		this.headerInfo.add(contentTypeHeader);
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
