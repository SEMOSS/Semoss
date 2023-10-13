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

public class TrimLambda extends AbstractMapLambda {

	private int numCols;
	// store indices of column values
	private List<Integer> colIndices;
	// dynamically create the trim column names
	private String[] trimColumnArr;
	
	@Override
	public IHeadersDataRow process(IHeadersDataRow row) {
		Object[] values = row.getValues();
		
		Object[] trimValues = new Object[numCols];
		for(int i = 0; i < numCols; i++) {
			int indexToGet = colIndices.get(i).intValue();
			trimValues[i] = values[indexToGet].toString().trim();
		}
		
		// copy the row so we dont mess up references
		IHeadersDataRow rowCopy = row.copy();
		rowCopy.addFields(this.trimColumnArr, trimValues);
		return rowCopy;
	}
	
	@Override
	public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
		this.headerInfo = headerInfo;
		
		// figure out which indices are those we want to use
		List<String> colNames = new ArrayList<String>();
		this.colIndices = new ArrayList<Integer>();
		int totalCols = headerInfo.size();
		int inputCols = columns.size();
		
		// this modifies the header info map by reference
		NEXT_COLUMN : for(int i = 0; i < inputCols; i++) {
			String valueToFind = columns.get(i);
			for(int j = 0; j < totalCols; j++) {
				Map<String, Object> headerMap = headerInfo.get(j);
				String alias = headerMap.get("alias").toString();
				if(alias.equals(valueToFind)) {
					colNames.add("TRIM_" + alias);
					this.colIndices.add(new Integer(j));
					continue NEXT_COLUMN;
				}
			}
			
			// throw an error
			throw new SemossPixelException(
					new NounMetadata("Could not find column " + valueToFind + " in Trim routine",
							PixelDataType.CONST_STRING, 
							PixelOperationType.ERROR));
		}
		
		this.numCols = colIndices.size();
		this.trimColumnArr = new String[numCols];
		
		// add all the trim columns
		for(int i = 0; i < numCols; i++) {
			String colName = colNames.get(i);
			this.trimColumnArr[i] = colName;
			// need to add a new entity for the column
			Map<String, Object> headerMap = new HashMap<String, Object>();
			headerMap.put("alias", colName);
			headerMap.put("header", colName);
			headerMap.put("type", "STRING");
			headerMap.put("derived", true);
			this.headerInfo.add(headerMap);
		}
	}
}
