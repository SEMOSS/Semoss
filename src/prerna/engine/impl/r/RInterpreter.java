package prerna.engine.impl.r;

import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.IMetaData;
import prerna.ds.QueryStruct;
import prerna.rdf.query.builder.IQueryInterpreter;

public class RInterpreter implements IQueryInterpreter {

	private QueryStruct qs;
	
	private String dataTableName = "datatable";
	private Map<String, IMetaData.DATA_TYPES> colDataTypes;
	
	// keep track of the filters
	// do this in builder since it can be very large
	private StringBuilder filterCriteria;
	
	// keep the columns to export
	private String selectors;
	
	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}
	
	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}

	@Override
	public String composeQuery() {
		if(colDataTypes == null) {
			colDataTypes = new Hashtable<String, IMetaData.DATA_TYPES>();
		}
		
		// note, that the join info in the QS has no meaning for a R frame as 
		// we cannot connect across data tables
		
		addFilters();
		addSelectors();
		
		StringBuilder query = new StringBuilder();
		query.append(this.dataTableName).append("[ ").append(this.filterCriteria.toString()).append(", ").append(this.selectors).append(", with=FALSE ]");
		
		return query.toString();
	}

	
	private void addSelectors() {
		Hashtable<String, Vector<String>> selectors = qs.selectors;
		Set<String> uniqueSet = new LinkedHashSet<String>();
		for(String col : selectors.keySet()) {
			uniqueSet.add(col);
			uniqueSet.addAll(selectors.get(col));
		}
		
		this.selectors = createStringRColVec(uniqueSet.toArray(new String[]{}));
	}

	private void addFilters() {
		// iterate through all the filters
		Hashtable<String, Hashtable<String, Vector>> filterHash = qs.andfilters;
		
		// grab each column to be filtered
		for(String colName : filterHash.keySet()) {
			// grab the comp hash
			Hashtable<String, Vector> compHash = filterHash.get(colName);
			for(String comparator : compHash.keySet()) {
				// actually add the filter logic
				addFilter(colName, comparator, compHash.get(comparator));
			}
		}
	}

	private void addFilter(String colName, String comparator, Vector vector) {
		if(filterCriteria == null) {
			// first time, this is null
			filterCriteria = new StringBuilder();
		} else {
			// need to aggregate the new filter criteria
			filterCriteria.append(" & ");
		}
		
		if(comparator.equals("=") || comparator.equals("==")) {
			filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" %in% ").append( createRColVec(vector, colDataTypes.get(colName))  );
		} else if(comparator.equals("!=")) {
			filterCriteria.append("!( ").append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" %in% ").append( createRColVec(vector, colDataTypes.get(colName)) ).append(")");
		} else {
			// these are some math operations
			filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" ").append(comparator).append(" ").append( createRColVec(vector, colDataTypes.get(colName))  );
		}
	}
	
	/**
	 * Convert a r vector from a java vector
	 * @param row				The object[] to convert
	 * @param dataType			The data type for each entry in the object[]
	 * @return					String containing the equivalent r column vector
	 */
	private String createRColVec(Vector<Object> row, IMetaData.DATA_TYPES dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.size();
		for(; i < size; i++) {
			if(IMetaData.DATA_TYPES.STRING == dataType) {
				str.append("\"").append(row.get(i)).append("\"");
			} else {
				// just in case this is not defined yet...
				// see the type of the value and add it in based on that
				if(dataType == null) {
					if(row.get(i) instanceof String) {
						str.append("\"").append(row.get(i)).append("\"");
					} else {
						str.append(row.get(i));
					}
				} else {
					str.append(row.get(i));
				}
			}
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	
	private String createStringRColVec(String[] selectors) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = selectors.length;
		for(; i < size; i++) {
			str.append("\"").append(selectors[i]).append("\"");
			// if not the last entry, append a "," to separate entries
			if( (i+1) != size) {
				str.append(",");
			}
		}
		str.append(")");
		return str.toString();
	}
	

	@Override
	public void setPerformCount(boolean performCount) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isPerformCount() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

}
