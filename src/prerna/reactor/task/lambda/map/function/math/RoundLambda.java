package prerna.reactor.task.lambda.map.function.math;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RoundLambda extends AbstractMapLambda {

	private static final String DECIMALS = "numDec";
	
	// store index of the column value
	private int colIndex;
	// dynamically create the round column name
	private String roundColumnName;
	
	// store the number of decimals to keep
	private int numDec;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();
		
		Number value = getRoundValue(values[colIndex], this.numDec);
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.roundColumnName, value);
		return rowCopy;
	}
	
	private Number getRoundValue(Object val, int numDec) {
		boolean retInt = (numDec == 0);
		try {
			if(val instanceof Number) {
				if(retInt) {
					return Math.round( ((Number) val).doubleValue() );
				}
				
				String formatStr = "#.";
				for(int i = 0; i < numDec; i++) {
					formatStr += "#";
				}
				DecimalFormat df = new DecimalFormat(formatStr);
				df.setRoundingMode(RoundingMode.HALF_UP);
				
				return Double.parseDouble(df.format(val));
			} else if(val instanceof String) {
				return getRoundValue(Double.parseDouble(val.toString()), numDec);
			}
		} catch(Exception e) {
			// ignore
		}
		return null;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;

		// figure out which indices are those we want to use
		int totalCols = headerInfo.size();
		int inputCols = columns.size();

		if(inputCols == 0) {
			throw new SemossPixelException(
					new NounMetadata("No column input found in Round", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		if(inputCols > 1) {
			throw new SemossPixelException(
					new NounMetadata("Can only input 1 column into Round", 
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}

		// this modifies the header info map by reference
		String valueToFind = columns.get(0);
		for(int j = 0; j < totalCols; j++) {
			Map<String, Object> headerMap = headerInfo.get(j);
			String alias = headerMap.get("alias").toString();
			if(alias.equals(valueToFind)) {
				this.roundColumnName = "ROUND_" + alias;
				this.colIndex = j;
				break;
			}
		}

		if(this.roundColumnName == null) {
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Could not find column " + valueToFind + " in Round routine",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.roundColumnName);
		headerMap.put("header", this.roundColumnName);
		headerMap.put("type", "NUMBER");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		// get the parameters
		if(this.params.containsKey(DECIMALS)) {
			this.numDec = ((Number) this.params.get(DECIMALS)).intValue();
		}
		// else we remove all decimals
	}
}
