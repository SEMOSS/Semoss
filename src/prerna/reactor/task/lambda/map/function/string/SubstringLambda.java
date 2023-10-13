package prerna.reactor.task.lambda.map.function.string;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubstringLambda extends AbstractMapLambda {

	private static final String START_INDEX = "start";
	private static final String END_INDEX = "end";

	// store indices of column to use
	private int colIndex;
	// dynamically create the substring column name
	private String substringColumn;
	
	private int startIndex;
	private int endIndex = -1;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();

		String subsetValue = "";
		try {
			if(endIndex >= 0) {
				subsetValue = values[colIndex].toString().substring(this.startIndex, this.endIndex);
			} else {
				subsetValue = values[colIndex].toString().substring(this.startIndex);
			}
		} catch(Exception e) {
			// ignore
		}

		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.substringColumn, subsetValue);
		return rowCopy;
	}

	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;

		// figure out which indices are those we want to use
		int totalCols = headerInfo.size();
		int inputCols = columns.size();

		if(inputCols == 0) {
			throw new SemossPixelException(
					new NounMetadata("No column input found in Substring", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		if(inputCols > 1) {
			throw new SemossPixelException(
					new NounMetadata("Can only input 1 column into Substring", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		// this modifies the header info map by reference
		String valueToFind = columns.get(0);
		for(int j = 0; j < totalCols; j++) {
			Map<String, Object> headerMap = headerInfo.get(j);
			String alias = headerMap.get("alias").toString();
			if(alias.equals(valueToFind)) {
				this.substringColumn = "SUBSTR_" + alias;
				this.colIndex = j;
				break;
			}
		}

		if(this.substringColumn == null) {
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Could not find column " + valueToFind + " in Substring routine",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.substringColumn);
		headerMap.put("header", this.substringColumn);
		headerMap.put("type", "STRING");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		// get the parameters
		this.startIndex = ((Number) this.params.get(START_INDEX)).intValue();
		if(this.params.containsKey(END_INDEX)) {
			this.endIndex = ((Number) this.params.get(END_INDEX)).intValue();
		}

	}
}
