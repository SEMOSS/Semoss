package prerna.sablecc2.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleSentimentAnalyzer;
import prerna.om.SentimentAnalysis;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleSentimentAnalyzerLambda extends AbstractFlatMapLambda {

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
		// grab the column sindex we want to use as the address
		Hashtable params = new Hashtable();
		Hashtable docParam = new Hashtable();
		docParam.put("type", "PLAIN_TEXT");
		docParam.put("language", "EN");
		docParam.put("content", value.toString().replace("_", " "));
		params.put("document", docParam);
		
		// construct new values to append onto the row
		// add new headers
		String[] newHeaders = new String[]{"sentence", "magnitude", "score"};
		
		List<IHeadersDataRow> retList = new Vector<IHeadersDataRow>();
		try {
			// loop through the results
			GoogleSentimentAnalyzer goog = new GoogleSentimentAnalyzer();
			Object resultObj = goog.execute(this.user, params);
			if(resultObj instanceof List) {
				List<SentimentAnalysis> results = (List<SentimentAnalysis>) resultObj;
				for(int i = 0; i < results.size(); i++) {
					SentimentAnalysis sentiment = results.get(i);
					processSentiment(sentiment, newHeaders, row, retList);
				}
			} else {
				SentimentAnalysis sentiment = (SentimentAnalysis) resultObj;
				processSentiment(sentiment, newHeaders, row, retList);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		return retList;
	}
	
	/**
	 * Process a sentiment
	 * @param sentiment
	 * @param newHeaders
	 * @param curRow
	 * @param retList
	 */
	private void processSentiment(SentimentAnalysis sentiment, String[] newHeaders, IHeadersDataRow curRow, List<IHeadersDataRow> retList) {
		Object[] newValues = new Object[3];
		newValues[0] = sentiment.getSentence();
		if(newValues[0] != null) {
			newValues[0] = newValues[0].toString()
					.replace("\n", " *LINE BREAK* ")
					.replace("\r", " *LINE BREAK* ")
					.replace("\t", " ")
					.replace("\"", "");
		}
		newValues[1] = sentiment.getMagnitude();
		newValues[2] = sentiment.getScore();
		
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
		Map<String, Object> sentenceHeader = getBaseHeader("sentence", "STRING");
		this.headerInfo.add(sentenceHeader);
		Map<String, Object> magnitudeHeader = getBaseHeader("magnitude", "NUMBER");
		this.headerInfo.add(magnitudeHeader);
		Map<String, Object> scoreHeader = getBaseHeader("score", "NUMBER");
		this.headerInfo.add(scoreHeader);
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
