package prerna.reactor.frame.filter;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class FrameFilterEmptyValuesReactor extends AddFrameFilterReactor {

	public FrameFilterEmptyValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	/**
	 * Overriding as this will always add the same filter conditions
	 * @return
	 */
	protected GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		List<String> columns = getColumns();
		for(String col : columns) {
			List<Object> values = new Vector<Object>();
			values.add("");
			values.add(null);
			grf.addFilters(SimpleQueryFilter.makeColToValFilter(col, "!=", values));
		}
		return grf;
	}
	
	/**
	 * Grab the column values
	 * @return
	 */
	private List<String> getColumns() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[0]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				List<Object> values = columnGrs.getAllValues();
				List<String> strValues = new Vector<String>();
				for(Object obj : values) {
					strValues.add(obj.toString());
				}
				return strValues;
			}
		}
		
		// else, we assume it is column values in the curRow
		List<Object> values = this.curRow.getAllValues();
		List<String> strValues = new Vector<String>();
		for(Object obj : values) {
			strValues.add(obj.toString());
		}
		return strValues;
	}

}
