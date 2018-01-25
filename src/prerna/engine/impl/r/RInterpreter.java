package prerna.engine.impl.r;

import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.QueryStruct;
import prerna.ds.r.RSyntaxHelper;
import prerna.rdf.query.builder.IQueryInterpreter;

public class RInterpreter implements IQueryInterpreter {

	private QueryStruct qs;
	
	private String dataTableName = "datatable";
	private Map<String, SemossDataType> colDataTypes;
	
	// keep track of the filters
	// do this in builder since it can be very large
	private StringBuilder filterCriteria = new StringBuilder();
	
	// keep the columns to export
	private String selectors = "";
	
	
	@Override
	public void setQueryStruct(QueryStruct qs) {
		this.qs = qs;
	}
	
	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}
	
	public void setColDataTypes(Map<String, SemossDataType> colDataTypes) {
		this.colDataTypes = colDataTypes;
	}
	
	@Override
	public String composeQuery() {
		if(colDataTypes == null) {
			colDataTypes = new Hashtable<String, SemossDataType>();
		}
		
		// note, that the join info in the QS has no meaning for a R frame as 
		// we cannot connect across data tables
		
		StringBuilder query = new StringBuilder();
		
		if(this.qs != null) {
			addFilters();
			addSelectors();
			query.append(this.dataTableName).append("[ ").append(this.filterCriteria.toString()).append(", ").append(this.selectors).append(", with=FALSE ]");
		}
		
		return query.toString();
	}
	
	public StringBuilder getFilerCriteria() {
		return this.filterCriteria;
	}

	
	private void addSelectors() {
		Map<String, List<String>> selectors = qs.selectors;
		Set<String> uniqueSet = new LinkedHashSet<String>();
		for(String col : selectors.keySet()) {
			uniqueSet.add(col);
			List<String> otherCols = selectors.get(col);
			for(String other : otherCols) {
				if(other.equals(QueryStruct.PRIM_KEY_PLACEHOLDER)) {
					continue;
				}
				uniqueSet.add(other);
			}
		}
		
		this.selectors = RSyntaxHelper.createStringRColVec(uniqueSet.toArray(new Object[]{}));
	}

	private void addFilters() {
		// iterate through all the filters
		Map<String, Map<String, List>> filterHash = qs.andfilters;
		
		// grab each column to be filtered
		for(String colName : filterHash.keySet()) {
			// grab the comp hash
			Map<String, List> compHash = filterHash.get(colName);
			for(String comparator : compHash.keySet()) {
				// actually add the filter logic
				addFilter(colName, comparator, compHash.get(comparator));
			}
		}
	}

	private void addFilter(String colName, String comparator, List vector) {
		if(filterCriteria.length() > 0) {
			// need to aggregate the new filter criteria
			filterCriteria.append(" & ");
		}
		
		//query is different if the data is date data type
		//for dates, the query needs to be written as datatable$col == X | datatable$col == Y | datatable$col == Z
		if (colDataTypes.get(colName) == SemossDataType.DATE) {
			if (comparator.equals("=") || comparator.equals("==")) {
				filterCriteria.append("(");
				for (int i = 0; i < vector.size(); i++) {
					filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
					.append("==").append(formatFilterValue(vector.get(i), colDataTypes.get(colName)));
					if ((i+1) < vector.size()) {
						filterCriteria.append(" | ");
					}
				}
				filterCriteria.append(")");

			} else if (comparator.equals("!=")) {
				for (int i = 0; i < vector.size(); i++) {
					filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
					.append("!=").append(formatFilterValue(vector.get(i), colDataTypes.get(colName)));
					if ((i+1) < vector.size()) {
						filterCriteria.append(" & ");
					}
				}
			}
			else {
				filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" ").append(comparator).append(" ").append( createRColVec(vector, colDataTypes.get(colName))  );
			}
		}
		
		//compose query for all situations where the data type is not a date
		//query should be of the form: 
		//for a single item:  datatable$col == X
		//for multiple conditions: datatable$col %in% c(X, Y, Z)
		else if(comparator.equals("=") || comparator.equals("==")) {
			if (vector.size() > 1) {
				filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" %in% ").append( createRColVec(vector, colDataTypes.get(colName))  );
			} else {
				filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" == ").append( formatFilterValue(vector.get(0), colDataTypes.get(colName))  );
			}
		} else if(comparator.equals("!=")) {
			if (vector.size() > 1) {
				filterCriteria.append("!( ").append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" %in% ").append( createRColVec(vector, colDataTypes.get(colName)) ).append(")");
			} else {
				filterCriteria.append(this.dataTableName).append("$").append(colName).append(" ")
				.append(" != ").append( formatFilterValue(vector.get(0), colDataTypes.get(colName))  );
			}
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
	private String createRColVec(List<Object> row, SemossDataType dataType) {
		StringBuilder str = new StringBuilder("c(");
		int i = 0;
		int size = row.size();
		for(; i < size; i++) {
			if(SemossDataType.STRING == dataType) {
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

	private String formatFilterValue(Object value, SemossDataType dataType) {
		if(SemossDataType.STRING == dataType || SemossDataType.DATE == dataType) {
			return "\"" + value + "\"";
		} else {
			// just in case this is not defined yet...
			// see the type of the value and add it in based on that
			if(dataType == null) {
				if(value instanceof String) {
					return "\"" + value + "\"";
				} else {
					return value + "";
				}
			} else {
				return value + "";
			}
		}
	}
	
//	private String createStringRColVec(String[] selectors) {
//		StringBuilder str = new StringBuilder("c(");
//		int i = 0;
//		int size = selectors.length;
//		for(; i < size; i++) {
//			str.append("\"").append(selectors[i]).append("\"");
//			// if not the last entry, append a "," to separate entries
//			if( (i+1) != size) {
//				str.append(",");
//			}
//		}
//		str.append(")");
//		return str.toString();
//	}
	

	@Override
	public void setPerformCount(int performCount) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int isPerformCount() {
		return QueryStruct.NO_COUNT;
	}

	@Override
	public void clear() {
		
	}
	
	
	public static void main(String[] args) {
		QueryStruct qs = new QueryStruct();
		qs.addSelector("Title", null);
		qs.addSelector("Nominated", null);
		qs.addSelector("Movie_Budget", null);

		Vector filterData1 = new Vector<>();
		filterData1.add("American Hustle");
		filterData1.add("Captain Phillips");
		qs.addFilter("Title", "=", filterData1);
		
		Vector filterData2 = new Vector<>();
		filterData2.add(50000000);
		qs.addFilter("Movie_Budget", ">", filterData2);
		
		RInterpreter rI = new RInterpreter();
		rI.setQueryStruct(qs);
		
		Map<String, SemossDataType> colDataTypes = new Hashtable<String, SemossDataType>();
		colDataTypes.put("Title", SemossDataType.STRING);
		colDataTypes.put("Nominated", SemossDataType.STRING);
		colDataTypes.put("Movie_Budget", SemossDataType.DOUBLE);
		rI.setColDataTypes(colDataTypes);
		
		String query = rI.composeQuery();
		System.out.println(query);
	}

}
