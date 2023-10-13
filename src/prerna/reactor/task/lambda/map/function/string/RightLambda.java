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

public class RightLambda extends AbstractMapLambda {

	private static final String INDEX = "index";

	// store indices of column to use
	private int colIndex;
	// dynamically create the right column names
	private String rightColumn;
	
	private int index;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();

		String subsetValue = "";
		try {
			subsetValue = values[colIndex].toString().substring(0, this.index);
		} catch(Exception e) {
			// ignore
		}

		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.rightColumn, subsetValue);
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
					new NounMetadata("No column input found in Right", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		if(inputCols > 1) {
			throw new SemossPixelException(
					new NounMetadata("Can only input 1 column into Right", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		// this modifies the header info map by reference
		String valueToFind = columns.get(0);
		for(int j = 0; j < totalCols; j++) {
			Map<String, Object> headerMap = headerInfo.get(j);
			String alias = headerMap.get("alias").toString();
			if(alias.equals(valueToFind)) {
				this.rightColumn = "RIGHT_" + alias;
				this.colIndex = j;
				break;
			}
		}

		if(this.rightColumn == null) {
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Could not find column " + valueToFind + " in Right routine",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.rightColumn);
		headerMap.put("header", this.rightColumn);
		headerMap.put("type", "STRING");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		// get the parameters
		if(this.params.containsKey(INDEX)) {
			this.index = ((Number) this.params.get(INDEX)).intValue();
		} else {
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Must provide the index value for the function",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

	}
}
