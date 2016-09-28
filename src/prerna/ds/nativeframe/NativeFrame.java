package prerna.ds.nativeframe;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData;
import prerna.ds.H2.H2Builder;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.Utility;

public class NativeFrame extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(NativeFrame.class.getName());
	NativeFrameBuilder builder;

	public NativeFrame() {
		this.metaData = new TinkerMetaData();
		this.builder = new NativeFrameBuilder();
	}

	// added as a path to get connection url for current dataframe
	public NativeFrameBuilder getBuilder() {
		return this.builder;
	}

	public void setConnection(String engineName) {
		this.builder.setConnection(engineName);
//		Connection connection = this.builder.getConnection();
		
//		if (connection != null) {
//			try {
//				// working with Mairiadb
//				Statement stmt = connection.createStatement();
//				String query = "select * from director";
//				ResultSet rs = stmt.executeQuery(query);
//				while (rs.next()) {
//				 System.out.print(rs.toString());
//				}
//
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
	}



	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		return null;
	}

	@Override
	public Iterator<Object[]> iterator() {
		return this.builder.buildIterator(getSelectors());
	}

	@Override
	public Iterator<Object[]> iterator(Map<String, Object> options) {
		String sortBy = (String) options.get(TinkerFrame.SORT_BY);
		String actualSortBy = null;

		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		List<String> selectorValues = new Vector<String>();
		for (String name : selectors) {
			if (name.startsWith(TinkerFrame.PRIM_KEY)) {
				continue;
			} else {
				if (name.equals(sortBy)) {
					actualSortBy = name;
				}
				String uniqueName = name; 
				if (uniqueName == null)
					uniqueName = name;
				selectorValues.add(uniqueName);
			}
		}
		options.put(TinkerFrame.SELECTORS, selectorValues);

		Map<Object, Object> temporalBindings = (Map<Object, Object>) options.get(TinkerFrame.TEMPORAL_BINDINGS);
		// clean values always put into list so bifurcation in logic doesn't
		// need to exist elsewhere
		Map<String, List<Object>> cleanTemporalBindings = new Hashtable<String, List<Object>>();
		if (temporalBindings != null) {
			for (Object key : temporalBindings.keySet()) {
				String cleanKey = key+"";

				Object val = temporalBindings.get(key);
				List<Object> cleanVal = new Vector<Object>();
				// if passed back a list
				if (val instanceof Collection) {
					Collection<? extends Object> collectionVal = (Collection<? extends Object>) val;
					for (Object valObj : collectionVal) {
						Object cleanObj = null;
						String strObj = valObj.toString().trim();
						String type = Utility.findTypes(strObj)[0] + "";
						if (type.equalsIgnoreCase("Date")) {
							cleanObj = Utility.getDate(strObj);
						} else if (type.equalsIgnoreCase("Double")) {
							cleanObj = Utility.getDouble(strObj);
						} else {
							cleanObj = Utility.cleanString(strObj, true, true,
									false);
						}
						((Vector) cleanVal).add(cleanObj);
					}
					cleanTemporalBindings.put(cleanKey, cleanVal);
				} else {
					// this means it is a single value
					Object cleanObj = null;
					String strObj = val.toString().trim();
					String type = Utility.findTypes(strObj)[0] + "";
					if (type.equalsIgnoreCase("Date")) {
						cleanObj = Utility.getDate(strObj);
					} else if (type.equalsIgnoreCase("Double")) {
						cleanObj = Utility.getDouble(strObj);
					} else {
						cleanObj = Utility.cleanString(strObj, true, true,
								false);
					}
					cleanVal.add(cleanObj);
					cleanTemporalBindings.put(cleanKey, cleanVal);
				}
			}
		}
		options.put(TinkerFrame.TEMPORAL_BINDINGS, cleanTemporalBindings);

		// if(selectors != null) {
		// List<String> h2selectors = new ArrayList<>();
		// for(String selector : selectors) {
		// h2selectors.add(H2HeaderMap.get(selector));
		// }
		// options.put(TinkerFrame.SELECTORS, h2selectors);
		// }

		if (actualSortBy != null) {
			options.put(TinkerFrame.SORT_BY, actualSortBy);
		}
		return builder.buildIterator(options);
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, Map<String, Object> options) {
		return null;
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean iterateAll) {
		return null;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		return null;
	}





	/**
	 * String columnHeader - the column on which to filter on filterValues - the
	 * values that will remain in the
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		if (filterValues != null && filterValues.size() > 0) {
			this.metaData.setFiltered(columnHeader, true);
			builder.setFilters(columnHeader, filterValues, H2Builder.Comparator.EQUAL);
		}
	}
	
	@Override
	public void filter(String columnHeader, Map<String, List<Object>> filterValues) {
		if(columnHeader == null || filterValues == null) return;

		DATA_TYPES type = this.metaData.getDataType(columnHeader);
		boolean isOrdinal = type != null && (type == DATA_TYPES.DATE || type == DATA_TYPES.NUMBER);


		String[] comparators = filterValues.keySet().toArray(new String[]{});
		for(int i = 0; i < comparators.length; i++) {
			String comparator = comparators[i];
			boolean override = i == 0;
			List<Object> filters = filterValues.get(comparator);

			comparator = comparator.trim();
			if(comparator.equals("=")) {

				if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.EQUAL);
				else builder.addFilters(columnHeader, filters, H2Builder.Comparator.EQUAL);

			} else if(comparator.equals("!=")) { 

				if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.NOT_EQUAL);
				else builder.addFilters(columnHeader, filters, H2Builder.Comparator.NOT_EQUAL);

			} else if(comparator.equals("<")) {

				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.LESS_THAN);
					else builder.addFilters(columnHeader, filters, H2Builder.Comparator.LESS_THAN);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}

			} else if(comparator.equals(">")) {

				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.GREATER_THAN);
					else builder.addFilters(columnHeader, filters, H2Builder.Comparator.GREATER_THAN);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}

			} else if(comparator.equals("<=")) {
				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.LESS_THAN_EQUAL);
					else builder.addFilters(columnHeader, filters, H2Builder.Comparator.LESS_THAN_EQUAL);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}
			} else if(comparator.equals(">=")) {
				if(isOrdinal) {

					if(override) builder.setFilters(columnHeader, filters, H2Builder.Comparator.GREATER_THAN_EQUAL);
					else builder.addFilters(columnHeader, filters, H2Builder.Comparator.GREATER_THAN_EQUAL);

				} else {
					throw new IllegalArgumentException(columnHeader
							+ " is not a numeric column, cannot use operator "
							+ comparator);
				}
			} else {
				// comparator not recognized...do equal by default? or do
				// nothing? or throw error?
			}
			this.metaData.setFiltered(columnHeader, true);
		}
	}

	@Override
	public void unfilter(String columnHeader) {
		this.metaData.setFiltered(columnHeader, false);
		builder.removeFilter(columnHeader);
	}

	@Override
	public void unfilter() {
		builder.clearFilters();
	}

	@Override
	public Object[] getFilterModel() {
		List<String> selectors = this.getSelectors();
		int length = selectors.size();
		Map<String, List<Object>> filteredValues = new HashMap<String, List<Object>>(length);
		Map<String, List<Object>> visibleValues = new HashMap<String, List<Object>>(length);
		Map<String, Map<String, Double>> minMaxValues = new HashMap<String, Map<String, Double>>(length);
		Iterator<Object[]> iterator = this.iterator();

		// put instances into sets to remove duplicates
		Set<Object>[] columnSets = new HashSet[length];
		for (int i = 0; i < length; i++) {
			columnSets[i] = new HashSet<Object>();
		}
		while (iterator.hasNext()) {
			Object[] nextRow = iterator.next();
			for (int i = 0; i < length; i++) {
				columnSets[i].add(nextRow[i]);
			}
		}

		//TODO: is this the same as filteredValues object?
		Map<String, List<Object>> h2filteredValues = builder.getFilteredValues(getSelectors());

		for (int i = 0; i < length; i++) {
			// get filtered values
			String h2key = selectors.get(i);//H2Builder.cleanHeader(selectors.get(i));
			List<Object> values = h2filteredValues.get(h2key);
			if (values != null) {
				filteredValues.put(selectors.get(i), values);
			} else {
			filteredValues.put(selectors.get(i), new ArrayList<Object>());
			}

			// get unfiltered values
			ArrayList<Object> unfilteredList = new ArrayList<Object>(columnSets[i]);
			visibleValues.put(selectors.get(i), unfilteredList);

			// store data type for header
			// get min and max values for numerical columns
			// TODO: need to include date type
			if(this.metaData.getDataType(selectors.get(i)) == IMetaData.DATA_TYPES.NUMBER) {
				Map<String, Double> minMax = new HashMap<String, Double>();

				// sort unfiltered array to pull relative min and max of unfiltered data
				Object[] unfilteredArray = unfilteredList.toArray();
				Arrays.sort(unfilteredArray);
				double absMin = getMin(selectors.get(i));
				double absMax = getMax(selectors.get(i));
				if(!unfilteredList.isEmpty()) {
					minMax.put("min", (Double)unfilteredArray[0]);
					minMax.put("max", (Double)unfilteredArray[unfilteredArray.length-1]);
				}
				minMax.put("absMin", absMin);
				minMax.put("absMax", absMax);

				// calculate how large each step in the slider should be
				double difference = absMax - absMin;
				double step = 1;
				if(difference < 1) {
					double tenthPower = Math.floor(Math.log10(difference));
					if(tenthPower < 0) {
						// ex. if difference is 0.009, step should be 0.001
						step = Math.pow(10, tenthPower);
					} else {
						step = 0.1;
					}
				}
				minMax.put("step", step);

				minMaxValues.put(selectors.get(i), minMax);
			}
		}

		return new Object[] { visibleValues, filteredValues, minMaxValues };
	}


	@Override
	public String getDataMakerName() {
		return "NativeFrame";
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.DATA_CONNECTDB, "prerna.sablecc.DataConnectDBReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.NativeApiReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.NativeImportDataReactor");

		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.MATH_PARAM, "prerna.sablecc.MathParamReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		
		return reactorNames;
	}
	
	public void createView(String selectQuery) {
		selectQuery = selectQuery.trim().toUpperCase();
		if(selectQuery == null || !selectQuery.startsWith("SELECT")) {
			throw new IllegalArgumentException("Query must be a 'SELECT' query");
		}
		String viewTable = this.builder.getNewTableName();
		selectQuery = "("+selectQuery+")";
//		selectQuery = "CREATE OR REPLACE VIEW "+viewTable+" AS "+selectQuery;
		selectQuery = "CREATE TEMPORARY TABLE "+viewTable+" AS "+selectQuery;
		try {
			builder.runExternalQuery(selectQuery);
			builder.setView(viewTable);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public void close() {
		try {
			this.builder.dropView();
			this.builder.getConnection().close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/******************************* UNNECESSARY ON NATIVE FRAME FOR NOW BUT NEED TO OVERRIDE FOR NOW *************************************************/
	
	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
	}
	
	@Override
	public void save(String fileName) {
	}

	@Override
	public ITableDataFrame open(String fileName, String userId) {
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow) {
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow) {
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToValMap) {
	}

	@Override
	public Map<String, Object[]> getFilterTransformationValues() {
		return null;
	}

	@Override
	public void removeColumn(String columnHeader) {
	}
	
	@Override
	public void addRow(Object[] rowCleanData) {
	}

	@Override
	public void addRow(Object[] cleanCells, String[] headers) {
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality, Map<String, String> logicalToValMap) {
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
	}


}
