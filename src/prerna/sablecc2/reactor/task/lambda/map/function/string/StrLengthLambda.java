package prerna.sablecc2.reactor.task.lambda.map.function.string;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.task.lambda.map.AbstractMapLambda;

public class StrLengthLambda extends AbstractMapLambda {

	private int numCols;
	// store indices of column values
	private List<Integer> colIndices;
	private List<Object> constantValues;
	// dynamically create the length column name
	private String strLenColumn;
	private String[] strLenColumnArr;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();
		
		int counter = 0;
		int len = 0;
		for(int i = 0; i < numCols; i++) {
			int indexToGet = colIndices.get(i).intValue();
			if(indexToGet >= 0) {
				len += values[indexToGet].toString().length();
			} else {
				len += this.constantValues.get(counter).toString().length();
				counter++;
			}
		}
		
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.strLenColumnArr, new Object[]{len});
		return rowCopy;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		
		// figure out which indices are those we want to convert to a double
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
					if(this.strLenColumn == null) {
						this.strLenColumn = "LEN_" + alias;
					} else {
						this.strLenColumn += "_" + alias;
					}
					this.colIndices.add(new Integer(j));
					continue NEXT_COLUMN;
				}
			}
			// if we got to this point, we have a header we did not find
			// so it must be a constant
			this.colIndices.add(new Integer(-1));
			this.constantValues.add(valueToFind);
			
			// also include it in the str length name
			if(this.strLenColumn == null) {
				this.strLenColumn = "LEN_" + valueToFind.replaceAll("\\s+", "_");
			} else {
				this.strLenColumn += "_" + valueToFind.replaceAll("\\s+", "_");
			}
		}
		
		this.strLenColumnArr = new String[]{this.strLenColumn};
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.strLenColumn);
		headerMap.put("header", this.strLenColumn);
		headerMap.put("type", "NUMBER");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		// get for convenience
		this.numCols = colIndices.size();
	}
}
