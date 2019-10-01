package prerna.ds.util.flatfile;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
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
		this.helper.setDelimiter(delimiter);
		this.helper.parse(qs.getFilePath());

		this.dataTypeMap = qs.getColumnTypes();
		this.newHeaders = qs.getNewHeaderNames();

		// set the user defined headers
		if (this.newHeaders != null && !this.newHeaders.isEmpty()) {
			this.helper.modifyCleanedHeaders(this.newHeaders);
		}

		setSelectors(qs.getSelectors());

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
		
		// set limit and offset
		this.limit = qs.getLimit();
		this.offset = qs.getOffset();
	}

	/**
	 * Determine the data types by parsing through the file
	 * @param fileIterator
	 */
	private void setUnknownTypes() {
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
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

	@Override
	public void getNextRow() {
		String[] row = helper.getNextRow();
		this.nextRow = row;
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
	public void reset() {
		this.helper.reset(false);
	}

	@Override
	public void cleanUp() {
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
