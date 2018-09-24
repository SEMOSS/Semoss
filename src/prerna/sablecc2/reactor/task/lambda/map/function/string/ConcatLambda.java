package prerna.sablecc2.reactor.task.lambda.map.function.string;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.reactor.task.lambda.map.AbstractMapLambda;

public class ConcatLambda extends AbstractMapLambda {

	private int numCols;
	// store indices of column values
	private List<Integer> colIndices;
	// store constants to concat
	private List<Object> constantValues;
	// dynamically create the concat column name
	private String concatColumn;
	private String[] concatColumnArr;
	
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
		rowCopy.addFields(concatColumnArr, new Object[]{concatValue});
		return rowCopy;		
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		
		// figure out which indices are those we want to convert to a double
		List<Integer> indices = new ArrayList<Integer>();
		List<Object> constantValues = new ArrayList<Object>();
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
					indices.add(new Integer(j));
					continue NEXT_COLUMN;
				}
			}
			// if we got to this point, we have a header we did not find
			// so it must be a constant
			indices.add(new Integer(-1));
			constantValues.add(valueToFind);
		}
		
		this.concatColumnArr = new String[]{this.concatColumn};
		
		// need to add a new entity for the column
		Map<String, Object> headerMap = new HashMap<String, Object>();
		headerMap.put("alias", this.concatColumn);
		headerMap.put("header", this.concatColumn);
		headerMap.put("type", "STRING");
		headerMap.put("derived", true);
		this.headerInfo.add(headerMap);
		
		this.colIndices = indices;
		this.constantValues = constantValues;
		this.numCols = colIndices.size();
	}
}
