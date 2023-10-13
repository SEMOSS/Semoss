package prerna.reactor.task.lambda.map.function.string;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.reactor.task.lambda.map.AbstractMapLambda;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ConcatLambda extends AbstractMapLambda {

	private int numCols;
	// store indices of column values
	private List<Integer> colIndices;
	// store constants to concat
	private List<Object> constantValues;
	// dynamically create the concat column name
	private String concatColumn;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();
		
		int counter = 0;
		String concatValue = "";
		for(int i = 0; i < numCols; i++) {
			int indexToGet = colIndices.get(i).intValue();
			if(indexToGet >= 0) {
				concatValue += values[indexToGet].toString();
			} else {
				concatValue += this.constantValues.get(counter);
				counter++;
			}
		}
		
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.concatColumn, concatValue);
		return rowCopy;		
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		
		// figure out which indices are those we want to use
		this.colIndices = new ArrayList<Integer>();
		this.constantValues = new ArrayList<Object>();
		int totalCols = headerInfo.size();
		int inputCols = columns.size();
		
		// this modifies the header info map by reference
		NEXT_COLUMN : for(int i = 0; i < inputCols; i++) {
			String valueToFind = columns.get(i);
			for(int j = 0; j < totalCols; j++) {
				Map<String, Object> headerMap = headerInfo.get(j);
				String alias = headerMap.get("alias").toString();
				if(alias.equals(valueToFind)) {
					if(this.concatColumn == null) {
						this.concatColumn = "Concat_" + alias;
					} else {
						this.concatColumn += "_" + alias;
					}
					this.colIndices.add(new Integer(j));
					continue NEXT_COLUMN;
				}
			}
			// if we got to this point, we have a header we did not find
			// so it must be a constant
			this.colIndices.add(new Integer(-1));
			constantValues.add(valueToFind);
		}
		
		// in case you want me to concat some random stuff
		// and do not pass in any values
		if(this.concatColumn == null) {
			if(constantValues.isEmpty()) {
				// throw error
				throw new SemossPixelException(
						new NounMetadata("No input recognized in concat", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.ERROR));
			}
			
			// loop through and append
			this.concatColumn = "Concat";
			for(Object o : constantValues) {
				this.concatColumn += "_" + o;
			}
		}
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.concatColumn);
		headerMap.put("header", this.concatColumn);
		headerMap.put("type", "STRING");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		// get for convenience
		this.numCols = colIndices.size();
	}
}
