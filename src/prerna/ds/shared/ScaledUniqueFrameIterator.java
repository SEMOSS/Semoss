package prerna.ds.shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ScaledUniqueFrameIterator implements Iterator<List<Object[]>> {

	private ITableDataFrame frame;
	private String uniqueColumnName;
	private QueryStruct2 qs;
	private Iterator<Object> valueIterator;
	
	public ScaledUniqueFrameIterator(
		ITableDataFrame frame, 
		String columnName,
		Double[] maxArr, 
		Double[] minArr,
		List<SemossDataType> dataTypes,
		List<String> selectors) {
		
		this.frame = frame;
		this.uniqueColumnName = columnName;
		Object[] column = frame.getColumn(columnName);
		this.valueIterator = Arrays.asList(column).iterator();
		
		// create the QS being used for querying
		this.qs = new QueryStruct2();
		
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			String unqiueSelectorName = selectors.get(i);
			
			QueryColumnSelector sColumn = new QueryColumnSelector();
			if(unqiueSelectorName.contains("__")) {
				String[] sSplit = unqiueSelectorName.split("__");
				sColumn.setTable(sSplit[0]);
				sColumn.setColumn(sSplit[1]);
			} else {
				sColumn.setTable(unqiueSelectorName);
				sColumn.setColumn(QueryStruct2.PRIM_KEY_PLACEHOLDER);
			}
			
			if(maxArr[i] != null && minArr[i] != null) {
				// we need to normalize this guy
				double range = maxArr[i] - minArr[i];
				
				QueryConstantSelector minConst = new QueryConstantSelector();
				minConst.setConstant(minArr[i]);
				
				QueryArithmeticSelector minusSelector = new QueryArithmeticSelector();
				minusSelector.setLeftSelector(sColumn);
				minusSelector.setRightSelector(minConst);
				minusSelector.setMathExpr("-");
				
				QueryConstantSelector rangeConst = new QueryConstantSelector();
				rangeConst.setConstant(range);
				
				QueryArithmeticSelector normalizeSelector = new QueryArithmeticSelector();
				normalizeSelector.setLeftSelector(minusSelector);
				normalizeSelector.setRightSelector(rangeConst);
				normalizeSelector.setMathExpr("/");
				
				qs.addSelector(normalizeSelector);
			} else {
				// we just add this as a column
				qs.addSelector(sColumn);
			}
		}
		
		// dont forget about filters
		qs.mergeFilters(frame.getFrameFilters());
	}
	
	@Override
	public boolean hasNext() {
		if(this.valueIterator.hasNext()) {
			return true;
		}
		return false;
	}

	@Override
	public List<Object[]> next() {
		if(hasNext()) {
			Object nextVal = valueIterator.next();
			SimpleQueryFilter instanceFilter = null;
			if(nextVal instanceof Number) {
				instanceFilter = new SimpleQueryFilter(
						new NounMetadata(new QueryColumnSelector(uniqueColumnName), PixelDataType.COLUMN), 
						"==", 
						new NounMetadata(nextVal, PixelDataType.CONST_DECIMAL)
						);
			} else {
				instanceFilter = new SimpleQueryFilter(
						new NounMetadata(new QueryColumnSelector(uniqueColumnName), PixelDataType.COLUMN), 
						"==", 
						new NounMetadata(nextVal, PixelDataType.CONST_STRING)
						);
			}
			
			// remove all previous filters
			this.qs.getFilters().removeColumnFilter(uniqueColumnName);
			// and add this one single value filter
			this.qs.addFilter(instanceFilter);
			
			List<Object[]> retData = new ArrayList<Object[]>();
			Iterator<IHeadersDataRow> it = frame.query(qs);
			while(it.hasNext()) {
				retData.add(it.next().getValues());
			}
			return retData;
		} else {
			throw new NoSuchElementException("No more elements"); 
		}
		
	}
}
