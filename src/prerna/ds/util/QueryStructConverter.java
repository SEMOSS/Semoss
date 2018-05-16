package prerna.ds.util;

import java.util.List;
import java.util.Map;

import prerna.ds.QueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryStructConverter {

	/*
	 * Class used to convert old qs to new qs
	 */
	
	private QueryStructConverter() {
		
	}
	
	public static SelectQueryStruct convertOldQueryStruct(QueryStruct qs) {
		SelectQueryStruct retQs = new SelectQueryStruct();

		// convert the selectors
		Map<String, List<String>> oldSelectors = qs.selectors;
		for(String table : oldSelectors.keySet()) {
			retQs.addSelector(new QueryColumnSelector(table));
			List<String> props = oldSelectors.get(table);
			for(int i = 0; i < props.size(); i++) {
				retQs.addSelector(new QueryColumnSelector(table + "__" + props.get(i)));
			}
		}

		// convert the filters
		Map<String, Map<String, List>> oldFilters = qs.andfilters;
		for(String colname : oldFilters.keySet()) {
			// need to determine if colname is a table or a column
			QueryColumnSelector colSelector = null;
			if(oldSelectors.keySet().contains(colname)) {
				colSelector = new QueryColumnSelector(colname);
			} else {
				// this is a gamble
				// hopefully the name doesn't repeat...
				// if it did, old logic would give an error anyway at anohter point in time
				for(String table : oldSelectors.keySet()) {
					List<String> propNames = oldSelectors.get(table);
					if(propNames.contains(colname)) {
						colSelector = new QueryColumnSelector(table + "__" + colname);
						break;
					}
				}
			}

			// now that i have the selector
			// i need to add all the filters
			Map<String, List> compHash = oldFilters.get(colname);
			for(String comparator : compHash.keySet()) {
				List values = compHash.get(comparator);
				PixelDataType type = null;
				if(values.get(0) instanceof Number) {
					type = PixelDataType.CONST_DECIMAL;
				} else {
					type = PixelDataType.CONST_STRING;
				}
				SimpleQueryFilter newFilter = new SimpleQueryFilter(
						new NounMetadata(colSelector, PixelDataType.COLUMN), 
						comparator, 
						new NounMetadata(compHash.get(comparator), type));
				retQs.addExplicitFilter(newFilter);
			}
		}

		// now add the relationships
		// this hasn't changed at all....
		// just set it
		retQs.setRelations(qs.getRelations());

		return retQs;
	}
}
