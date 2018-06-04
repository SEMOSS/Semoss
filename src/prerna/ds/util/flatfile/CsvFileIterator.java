package prerna.ds.util.flatfile;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ArrayUtilityMethods;

public class CsvFileIterator extends AbstractFileIterator {

	private CSVFileHelper helper;
	private CsvQueryStruct qs;

	public CsvFileIterator(CsvQueryStruct qs) {
		this.qs = qs;
		this.fileLocation = qs.getFilePath();
		char delimiter = qs.getDelimiter();

		// set default values
		this.helper = new CSVFileHelper();
		this.filters = new GenRowFilters();
		this.helper.setDelimiter(delimiter);
		this.helper.parse(qs.getFilePath());

		this.dataTypeMap = qs.getColumnTypes();
		this.newHeaders = qs.getNewHeaderNames();
		
		// set the user defined headers
		if (this.newHeaders != null && !this.newHeaders.isEmpty()) {
			this.helper.modifyCleanedHeaders(this.newHeaders);
		}

		setSelectors(qs.getSelectors());
		setFilters(qs.getExplicitFilters());
		
		// now that I have set the headers from the setSelectors
		this.headers = this.helper.getHeaders();
		this.additionalTypesMap = qs.getAdditionalTypes();

		if(this.dataTypeMap != null && !this.dataTypeMap.isEmpty()) {
			this.types = new SemossDataType[this.headers.length];
			this.additionalTypes = new String[this.headers.length];
			for(int index = 0; index < this.headers.length; index++) {
				this.types[index] = SemossDataType.convertStringToDataType(dataTypeMap.get(this.headers[index]));
				if(this.additionalTypesMap != null) {
					this.additionalTypes[index] = additionalTypesMap.get(this.headers[index]);
				}
			}

			this.helper.parseColumns(this.headers);
		} else {
			setUnknownTypes();
			setSelectors(qs.getSelectors());
			qs.setColumnTypes(this.dataTypeMap);
		}

		this.getNextRow(); // this will get the first row of the file
	}
	
	/**
	 * Determine the data types by parsing through the file
	 * @param fileIterator
	 */
	private void setUnknownTypes() {
		Map[] predictionMaps = CSVFileHelper.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		this.dataTypeMap = predictionMaps[0];
		this.additionalTypesMap = predictionMaps[1];
		
		// need to redo types to be only those in the selectors
		this.types = new SemossDataType[this.headers.length];
		this.additionalTypes = new String[this.headers.length];
		for(int i = 0; i < this.headers.length; i++) {
			this.types[i] = SemossDataType.convertStringToDataType(this.dataTypeMap.get(this.headers[i]));
			this.additionalTypes[i] = this.additionalTypesMap.get(this.headers[i]);
		}
	}
	
	public void getNextRow() {
		String[] row = helper.getNextRow();
		if(filters == null || filters.isEmpty()) {
			this.nextRow = row;
			return;
		}
		
		String[] newRow = null;
		while (newRow == null && (row != null)) {

			Set<String> allFilteredCols = this.filters.getAllFilteredColumns();
			//isValid checks if the row meets all of the given filters 
			boolean isValid = true;
			for (String col : allFilteredCols) {
				int rowIndex = Arrays.asList(headers).indexOf(col);
				// check valid index
				if (rowIndex >= 0) {
					//list of all filters on a given column
					List<SimpleQueryFilter> nextSet = this.filters.getAllSimpleQueryFiltersContainingColumn(col);
					for (SimpleQueryFilter filter : nextSet) {
						//get all filter information
						FILTER_TYPE filterType = filter.getFilterType();
						NounMetadata leftComp = filter.getLComparison();
						NounMetadata rightComp = filter.getRComparison();
						String comparator = filter.getComparator();

						if (filterType == FILTER_TYPE.COL_TO_COL) {
							//TODO
							//isValid = isValid && filterColToCol(leftComp, rightComp, row, comparator, rowIndex);
						} else if (filterType == FILTER_TYPE.COL_TO_VALUES) {
							// Genre = ['Action'] example
							isValid = isValid && filterColToValues(leftComp, rightComp, row, comparator, rowIndex);

						} else if (filterType == FILTER_TYPE.VALUES_TO_COL) {
							// here the left and rightcomps are reversed, so send them to the method in opposite order and reverse comparator
							// 50000 > MovieBudget gets sent as MovieBudget < 50000
							isValid = isValid && filterColToValues(rightComp, leftComp, row, IQueryFilter.getReverseNumericalComparator(comparator), rowIndex);
						} else if (filterType == FILTER_TYPE.VALUE_TO_VALUE) {
							//?????????
							
						}
					}
				}
			}
			if (isValid) {
				newRow = row;
				break;
			} else {
				newRow = null;
			}
			if (newRow == null) {
				row = helper.getNextRow();
			}
		}
		this.nextRow = newRow;
	}

	private void setSelectors(List<IQuerySelector> selectors) {
		if (selectors.isEmpty()) {
			// if no selectors, return everything
			String[] allHeaders = this.helper.getHeaders();
			for(int i = 0; i < allHeaders.length; i++) {
				QueryColumnSelector newSelector = new QueryColumnSelector("DND__" + allHeaders[i]);
				this.qs.addSelector(newSelector);
			}
			return; 
		}
	
		int numSelectors = selectors.size();
		
		String[] csvSelectors = new String[numSelectors];
		for(int i = 0; i < numSelectors; i++) {
			QueryColumnSelector newSelector = (QueryColumnSelector) selectors.get(i);
			if(newSelector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				throw new IllegalArgumentException("Cannot perform math on a csv import");
			}
			csvSelectors[i] = newSelector.getAlias();;
		}
		
		String[] allHeaders = this.helper.getHeaders();
		if(allHeaders.length != csvSelectors.length) {
			// order the selectors
			// all headers will be ordered
			String[] orderedSelectors = new String[csvSelectors.length];
			int counter = 0;
			for(String header : allHeaders) {
				if(ArrayUtilityMethods.arrayContainsValue(csvSelectors, header)) {
					orderedSelectors[counter] = header;
					counter++;
				}
			}
			
			this.helper.parseColumns(orderedSelectors);
			// after redoing the selectors, we need to skip the headers 
			this.helper.getNextRow(); 
		}
	}
	
	@Override
	public void resetHelper() {
		this.helper.reset(false);
	}

	@Override
	public void clearHelper() {
		this.helper.clear();
	}
	
	public CSVFileHelper getHelper() {
		return this.helper;
	}
	
	public CsvQueryStruct getQs() {
		return this.qs;
	}

	public void setQs(CsvQueryStruct qs) {
		this.qs = qs;
	}
}
