package prerna.ds.util.flatfile;

import java.io.IOException;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.helper.ParquetFileHelper;
import prerna.query.querystruct.ParquetQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ArrayUtilityMethods;

public class ParquetFileIterator extends AbstractFileIterator {

	private ParquetFileHelper helper;
	private ParquetQueryStruct qs;

	/**
	 * Simple iterator used when all the information can be parsed from the QS
	 * @param qs
	 */

	public ParquetFileIterator(ParquetQueryStruct qs) {
		this.qs = qs;
		this.fileLocation = qs.getFilePath();
		// set default values
		this.helper = new ParquetFileHelper();
		this.helper.setFileLocation(this.fileLocation);
		
		// dont think I need this
		//this.helper.parse(qs.getFilePath());

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
			// shouldnt have any unkown types because we stop this at the beginning
			//setUnknownTypes();
			setSelectors(qs.getSelectors());
			qs.setColumnTypes(this.dataTypeMap);
		}

		//this.getNextRow(); // this will get the first row of the file
		// set limit and offset
		this.limit = qs.getLimit();
		this.offset = qs.getOffset();
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
		String[] parquetSelectors = new String[numSelectors];
		for(int i = 0; i < numSelectors; i++) {
			QueryColumnSelector newSelector = (QueryColumnSelector) selectors.get(i);
			if(newSelector.getSelectorType() != IQuerySelector.SELECTOR_TYPE.COLUMN) {
				throw new IllegalArgumentException("Cannot perform math on a csv import");
			}
			parquetSelectors[i] = newSelector.getAlias();;
		}

		String[] allHeaders = this.helper.getHeaders();
		//String[] allHeaders = this.helper.getNewUniqueParquetHeaders().stream().toArray(String[]::new);

		
		if(allHeaders.length != parquetSelectors.length) {
			// order the selectors
			// all headers will be ordered
			String[] orderedSelectors = new String[parquetSelectors.length];
			int counter = 0;
			for(String header : allHeaders) {
				if(ArrayUtilityMethods.arrayContainsValue(parquetSelectors, header)) {
					orderedSelectors[counter] = header;
					counter++;
				}
			}

			this.helper.parseColumns(orderedSelectors);
			// after redoing the selectors, we need to skip the headers 
		}
	}

	@Override
	public void reset() throws Exception {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void getNextRow() {
		// TODO Auto-generated method stub
		
	}
	
	public ParquetQueryStruct getQs() {
		return this.qs;
	}
}