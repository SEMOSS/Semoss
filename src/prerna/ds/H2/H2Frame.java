package prerna.ds.H2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.IMatcher;
import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.H2.H2Builder.Join;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.r.RRunner;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class H2Frame extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(H2Frame.class.getName());

	H2Builder builder;

	// this was being used when we wanted the sql interpreter to create the
	// traverse query
	// IQueryInterpreter interp = new SQLInterpreter();
	RRunner r = null;
	Map<String, String> joinHeaders = new HashMap<>();

	public H2Frame(String[] headers) {
		this.headerNames = headers;
		this.metaData = new TinkerMetaData();
		setSchema();
	}

	public H2Frame() {
		this.metaData = new TinkerMetaData();
		setSchema();
	}

	//added as a path to get connection url for current dataframe
	public H2Builder getBuilder(){
		return this.builder;
	}

	private void setSchema() {
		if (this.builder == null) {
			this.builder = new H2Builder();
		}
		this.builder.setSchema(this.userId);
	}
	
//	public void setH2Joiner(H2Joiner joiner) {
//		this.joiner = joiner;
//	}

	@Override
	/**
	 * Setting the user id in the builder will automatically update the schema 
	 */
	public void setUserId(String userId) {
		super.setUserId(userId);
		this.setSchema();
	}

	/*************************** AGGREGATION METHODS *************************/

	public void addRowsViaIterator(Iterator<IHeadersDataRow> it) {
		// TODO: differences between the tinker meta and the flat meta stored in
		// the data frame
		// TODO: results in us being unable to get the table name
		if (builder.tableName == null) {
			builder.tableName = getTableNameForUniqueColumn(getColumnHeaders()[0]);
		}

		// we really need another way to get the data types....
		Map<String, IMetaData.DATA_TYPES> typesMap = this.metaData.getColumnTypes();
		builder.addRowsViaIterator(it, typesMap);
	}

	@Override
	public void addRow(Object[] rowCleanData) {
		addRow(rowCleanData, getColumnHeaders());
	}

	@Override
	public void addRow(Object[] cells, String[] headers) {
		// TODO: differences between the tinker meta and the flat meta stored in
		// the data frame
		// TODO: results in us being unable to get the table name
		if (builder.tableName == null) {
			builder.tableName = getTableNameForUniqueColumn(headers[0]);
		}
		String[] types = new String[headers.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = Utility.convertDataTypeToString(this.metaData
					.getDataType(headers[i]));
			// need to stringify everything
			cells[i] = cells[i] + "";
		}
		String[] stringArray = Arrays.copyOf(cells, cells.length,
				String[].class);

		// get table for headers
		this.addRow(builder.tableName, stringArray, headers, types);
	}

	// need to make this private if we are going with single table h2
	public void addRow(String tableName, String[] cells, String[] headers,
			String[] types) {
		String[] headerValues = new String[headers.length];
		for (int j = 0; j < headers.length; j++) {
			headerValues[j] = getValueForUniqueName(headers[j]);
		}

		this.builder.tableName = tableName;
		this.builder.addRow(tableName, cells, headerValues, types);
	}

	// TODO : this won't with main column table
	public String getTableNameForUniqueColumn(String uniqueName) {
		return this.metaData.getParentValueOfUniqueNode(uniqueName);
	}

	/************************** END AGGREGATION METHODS **********************/

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
		LOGGER.info("Processing Component..................................");

		List<ISEMOSSTransformation> preTrans = component.getPreTrans();
		Vector<Map<String, String>> joinColList = new Vector<Map<String, String>>();
		String joinType = null;
		for (ISEMOSSTransformation transformation : preTrans) {
			if (transformation instanceof JoinTransformation) {
				Map<String, String> joinMap = new HashMap<String, String>();
				String joinCol1 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_ONE_KEY);
				String joinCol2 = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.COLUMN_TWO_KEY);
				joinType = (String) ((JoinTransformation) transformation).getProperties()
						.get(JoinTransformation.JOIN_TYPE);
				joinMap.put(joinCol2, joinCol1); // physical in query struct
				// ----> logical in existing
				// data maker
				joinColList.add(joinMap);
			}
		}

		processPreTransformations(component, component.getPreTrans());
		long time1 = System.currentTimeMillis();
		LOGGER.info("	Processed Pretransformations: " + (time1 - startTime) + " ms");

		IEngine engine = component.getEngine();
		// automatically created the query if stored as metamodel
		// fills the query with selected params if required
		// params set in insightcreatrunner
		String query = component.fillQuery();

		String[] displayNames = null;
		if (query.trim().toUpperCase().startsWith("CONSTRUCT")) {
			// TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
			// tgdm.fillModel(query, engine, this);
		} else if (!query.equals(Constants.EMPTY)) {
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			String[] headers = wrapper.getDisplayVariables();
			// if component has data from which we can construct a meta model
			// then construct it and merge it
			boolean hasMetaModel = component.getQueryStruct() != null;
			if (hasMetaModel) {
				String[] startHeaders = getH2Headers();
				if (startHeaders == null) {
					startHeaders = new String[0];
				}
				Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
				Map[] retMap = this.mergeQSEdgeHash(edgeHash, engine, joinColList);

				// set the addRow logic to false
				boolean addRow = false;
				// if all the headers are accounted or the frame is empty, then
				// the logic should only be inserting
				// the values from the iterator into the frame
				if (allHeadersAccounted(startHeaders, headers, joinColList) || this.isEmpty()) {
					addRow = true;
				}
				if (addRow) {
					while (wrapper.hasNext()) {
						IHeadersDataRow ss = (IHeadersDataRow) wrapper.next();
						addRow(ss.getValues(), headers);
					}
				} else {
					processIterator(wrapper, wrapper.getDisplayVariables(), retMap[1], joinColList, joinType);
				}

				List<String> fullNames = this.metaData.getColumnNames();
				this.headerNames = fullNames.toArray(new String[fullNames.size()]);
			}

			// else default to primary key tinker graph
			else {
				displayNames = wrapper.getDisplayVariables();
				Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(displayNames);
				TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, getNode2ValueHash(edgeHash));
				List<String> fullNames = this.metaData.getColumnNames();
				this.headerNames = fullNames.toArray(new String[fullNames.size()]);
				while (wrapper.hasNext()) {
					this.addRow(wrapper.next());
				}
			}
		}
		// List<String> fullNames = this.metaData.getColumnNames();
		// this.headerNames = fullNames.toArray(new String[fullNames.size()]);

		long time2 = System.currentTimeMillis();
		LOGGER.info("	Processed Wrapper: " + (time2 - time1) + " ms");

		processPostTransformations(component, component.getPostTrans());
		processActions(component, component.getActions());

		long time4 = System.currentTimeMillis();
		LOGGER.info("Component Processed: " + (time4 - startTime) + " ms");
	}

	/**
	 * Determine if all the headers are taken into consideration within the
	 * iterator This helps to determine if we need to perform an insert vs. an
	 * update query to fill the frame
	 * 
	 * @param headers1
	 *            The original set of headers in the frame
	 * @param headers2
	 *            The new set of headers from the iterator
	 * @param joins
	 *            Needs to take into consideration the joins since we can join
	 *            on columns that do not have the same names
	 * @return
	 */
	private boolean allHeadersAccounted(String[] headers1, String[] headers2, List<Map<String, String>> joins) {
		if (headers1.length != headers2.length) {
			return false;
		}

		// add values to a set and compare
		Set<String> header1Set = new HashSet<>();
		Set<String> header2Set = new HashSet<>();

		// make a set with headers1
		for (String header : headers1) {
			header1Set.add(header);
		}

		// make a set with headers2
		for (String header : headers2) {
			header2Set.add(header);
		}

		// add headers1 headers to headers2set if there is a matching join and
		// remove the other header
		for (Map<String, String> join : joins) {
			for (String key : join.keySet()) {
				header2Set.add(key);
				header2Set.remove(join.get(key));
			}
		}

		// take the difference
		header2Set.removeAll(header1Set);

		// return true if header sets matched, false otherwise
		return header2Set.size() == 0;
	}

	/****************************** FILTER METHODS **********************************************/

	/**
	 * String columnHeader - the column on which to filter on filterValues - the
	 * values that will remain in the
	 */
	@Override
	public void filter(String columnHeader, List<Object> filterValues) {
		if (filterValues != null && filterValues.size() > 0) {
			this.metaData.setFiltered(columnHeader, true);
			columnHeader = this.getValueForUniqueName(columnHeader);
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
		columnHeader = this.getValueForUniqueName(columnHeader);
		builder.removeFilter(columnHeader);
	}

	@Override
	public void unfilter() {
		builder.clearFilters();
	}

	@Override
	/**
	 * This method returns the filter model for the graph in the form:
	 * 
	 * [
	 * 		{
	 * 			header_1 -> [UF_instance_01, UF_instance_02, ..., UF_instance_0N]
	 * 			header_2 -> [UF_instance_11, UF_instance_12, ..., UF_instance_1N]
	 * 			...
	 * 			header_M -> [UF_instance_M1, UF_instance_M2, ..., UF_instance_MN]
	 * 		}, 
	 * 
	 * 		{
	 * 			header_1 -> [F_instance_01, F_instance_02, ..., F_instance_0N]
	 * 			header_2 -> [F_instance_11, F_instance_12, ..., F_instance_1N]
	 * 			...
	 * 			header_M -> [F_instance_M1, F_instance_M2, ..., F_instance_MN]
	 * 		}	
	 * ]
	 * 
	 * First object in array is Map<String, List<String>> where each header points to the list of UNFILTERED or VISIBLE values for that header.
	 * Second object in array is Map<String, List<String>> where each header points to the list of FILTERED values for that header.
	 * Third object in array only exists if column has numerical data in format Map<String, Map<String, Double>> containing relative min/max and absolute min/max for column.
	 */
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
		Map<String, List<Object>> h2filteredValues = builder.getFilteredValues(getH2Selectors());

		for (int i = 0; i < length; i++) {
			// get filtered values
//			String h2key = H2Builder.cleanHeader(selectors.get(i));
			String h2key = getH2Header(selectors.get(i));
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
				double min = getMin(selectors.get(i));
				double max = getMax(selectors.get(i));
				
				double absMin = builder.getStat(selectors.get(i), "MIN", true);
				double absMax = builder.getStat(selectors.get(i), "MAX", true);

				minMax.put("min", min);
				minMax.put("max", max);

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
	
	public Map<String, Object[]> getFilterTransformationValues() {
		Map<String, Object[]> retMap = new HashMap<String, Object[]>();
		// get meta nodes that are tagged as filtered
		// Map<String, String> filters = this.metaData.getFilteredColumns();
		// Map<String, List<Object>> filteredData = this.builder.filterHash;
		//
		// for(String name: filters.keySet()){
		//
		// //for each filtered column
		// String h2Name = this.getValueForUniqueName(name);
		// retMap.put(name, filteredData.get(h2Name).toArray());
		// }

		return retMap;
	}

	/****************************** END FILTER METHODS ******************************************/

	@Override
	public Iterator<Object[]> iterator() {
		// List<String> h2selectors = new ArrayList<>();
		// for(String selector : getSelectors()) {
		// h2selectors.add(H2HeaderMap.get(selector));
		// }
		// QueryStruct struct = this.metaData.getQueryStruct(null);
		// for(String header : this.builder.filterHash.keySet()){
		// struct.addFilter(header, "=", this.builder.filterHash.get(header));
		// }
		// interp.setQueryStruct(struct);//
		// String query = interp.composeQuery();
		// return this.builder.buildIterator(query);
		return this.builder.buildIterator(getH2Selectors());
	}
	
	public Iterator<Object[]> iterator(boolean getRawData, boolean ignoreFilters) {
		if(!ignoreFilters) {
			return iterator();
		}
		else return this.builder.buildIterator(getH2Selectors(), ignoreFilters);
	}

	@Override
	public Iterator<Object[]> iterator(Map<String, Object> options) {
		// sort by
		String sortBy = (String) options.get(TinkerFrame.SORT_BY);
		String actualSortBy = null;

		List<String> selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
		List<String> selectorValues = new Vector<String>();
		for (String name : selectors) {
			if (name.startsWith(TinkerFrame.PRIM_KEY)) {
				continue;
			} else {
				if(sortBy == null) {
					sortBy = name;
				}
				if (name.equals(sortBy)) {
					actualSortBy = this.getValueForUniqueName(name);
				}
				String uniqueName = this.getValueForUniqueName(name);
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
				String cleanKey = this.getValueForUniqueName(key + "");

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

	public void applyGroupBy(String[] column, String newColumnName,
			String valueColumn, String mathType) {
		// column = H2HeaderMap.get(column);
		// valueColumn = H2HeaderMap.get(valueColumn);
		// newColumnName = H2HeaderMap.get(newColumnName);
		for (int i = 0; i < column.length; i++) {
			column[i] = this.getValueForUniqueName(column[i]);
		}
		valueColumn = this.getValueForUniqueName(valueColumn);
		newColumnName = this.getValueForUniqueName(newColumnName);
		builder.processGroupBy(column, newColumnName, valueColumn, mathType,
				getH2Headers());
	}

	@Override
	public Object[] getColumn(String columnHeader) {
		columnHeader = this.getValueForUniqueName(columnHeader);
		Object[] array = builder.getColumn(columnHeader, false);
		return array;
	}

	@Override
	public Integer getUniqueInstanceCount(String columnHeader) {
		columnHeader = this.getValueForUniqueName(columnHeader);
		return builder.getColumn(columnHeader, true).length;
	}

	@Override
	public Double getMin(String columnHeader) {
		// make sure its a number
		if (this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			columnHeader = this.getValueForUniqueName(columnHeader);
			return builder.getStat(columnHeader, "MIN", false);
		}
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		if (this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			columnHeader = this.getValueForUniqueName(columnHeader);
			return builder.getStat(columnHeader, "MAX", false);
		}
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, Map<String, Object> options) {
		List<String> selectors = null;
		Double[] max = null;
		Double[] min = null;
		if (options != null && options.containsKey(TinkerFrame.SELECTORS)) {
			// get the max and min values based on the order that is defined
			selectors = (List<String>) options.get(TinkerFrame.SELECTORS);
			int numSelected = selectors.size();
			max = new Double[numSelected];
			min = new Double[numSelected];
			for (int i = 0; i < numSelected; i++) {
				// TODO: think about storing this value s.t. we do not need to
				// calculate max/min with each loop
				max[i] = getMax(selectors.get(i));
				min[i] = getMin(selectors.get(i));
			}
		} else {
			// order of selectors will match order of max and min arrays
			selectors = getH2Selectors();
			max = getMax();
			min = getMin();
		}

		columnHeader = this.getValueForUniqueName(columnHeader);
		Map<String, String> headerTypes = this.getH2HeadersAndTypes();
		if (builder.tableName == null) {
			builder.tableName = getTableNameForUniqueColumn(this.headerNames[0]);
		}
		ScaledUniqueH2FrameIterator iterator = new ScaledUniqueH2FrameIterator(columnHeader, builder.tableName, builder, max, min, headerTypes, selectors);
		return iterator;
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if (isNumeric(columnHeader)) {
			columnHeader = this.getValueForUniqueName(columnHeader);
			Object[] array = builder.getColumn(columnHeader, false);

			List<Double> numericCol = new ArrayList<Double>();
			Iterator<Object> it = Arrays.asList(array).iterator();
			while (it.hasNext()) {
				Object row = it.next();
				try {
					Double dval = ((Number) row).doubleValue();
					numericCol.add(dval);
				} catch (NumberFormatException e) {

				}
			}

			return numericCol.toArray(new Double[] {});
		}

		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow) {
		Object[] origValues = null;
		String[] origColumnHeaders = null;	
		boolean multiUpdates = false;
		
		Set<String> keySet = cleanRow.keySet();
		Map<String, Object> adjustedCleanRow = new LinkedHashMap<String, Object>();
		
		//distinguish between new columns (cleanRow) and original columns (rawRow)
		//remove original columns from new columns and keep them separated
		//way to figure out if one new column is being updated or multiple
		//specifically done when figuring out better approach for col split operation
//		if(!cleanRow.equals(rawRow)){
//			//collect original columns and their respective values
//			origValues = new Object[rawRow.keySet().size()];
//			origColumnHeaders = rawRow.keySet().toArray(new String[] {});
//			
//			for (int i = 0; i < origColumnHeaders.length; i++) {
//				origValues[i] = rawRow.get(origColumnHeaders[i]);
//				origColumnHeaders[i] = H2Builder.cleanHeader(origColumnHeaders[i]);
//			}		
//						
//			Set<String> origKeySet = rawRow.keySet();
//			boolean exists = false;
//			for(String key : keySet){	
//				exists = false;
//				for(String origKey : origKeySet){
//					if(key.equals(origKey)){
//						exists = true;
//						multiUpdates = true;
//					}
//				}
//				if(!exists)
//					adjustedCleanRow.put(key, cleanRow.get(key));
//			}			
//			cleanRow = adjustedCleanRow;	
//		}
		
		// if the sets contain keys not in header names, remove them
		adjustedCleanRow = new LinkedHashMap<String, Object>();
		keySet = cleanRow.keySet();
		for (String key : keySet) {
			if (ArrayUtilityMethods.arrayContainsValue(headerNames, key)) {
				adjustedCleanRow.put(key, cleanRow.get(key));
			}
		}
		cleanRow = adjustedCleanRow;		
		
		Object[] values = new Object[cleanRow.keySet().size()];
		String[] columnHeaders = cleanRow.keySet().toArray(new String[] {});

		Arrays.sort(columnHeaders, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				int firstIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headerNames, o1);
				int secondIndex = ArrayUtilityMethods.arrayContainsValueAtIndex(headerNames, o2);
				if (firstIndex < secondIndex)
					return -1;
				if (firstIndex == secondIndex)
					return 0;
				else
					return 1;
			}

		});

		for (int i = 0; i < columnHeaders.length; i++) {
			values[i] = cleanRow.get(columnHeaders[i]);
			columnHeaders[i] = H2Builder.cleanHeader(columnHeaders[i]);
		}		
		
		if(multiUpdates)			
			builder.updateTable2(origColumnHeaders, origValues, columnHeaders, values);		
		else
			builder.updateTable(getH2Headers(), values, columnHeaders);
		
		if(this.isJoined()) {
			builder.joiner.refreshView(this, builder.getViewTableName());
		}
	}

	@Override
	public void removeColumn(String columnHeader) {
		if (!ArrayUtilityMethods.arrayContainsValue(this.headerNames,
				columnHeader)) {
			return;
		}

		// drop the column from the h2 table
		builder.dropColumn(this.getValueForUniqueName(columnHeader));
		// remove the column name from the metadata
		this.metaData.dropVertex(columnHeader);

		// update the headerNames array
		String[] newHeaders = new String[this.headerNames.length - 1];
		int newHeaderIdx = 0;
		for (int i = 0; i < this.headerNames.length; i++) {
			String name = this.headerNames[i];
			if (!name.equals(columnHeader)) {
				newHeaders[newHeaderIdx] = name;
				newHeaderIdx++;
			}
		}
		this.headerNames = newHeaders;
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean iterateAll) {

		// Map<String, Object> options = new HashMap<String, Object>();
		// options.put(DE_DUP, true);
		//
		// List<String> selectors = new ArrayList<String>();
		// selectors.add(columnHeader);
		// options.put(SELECTORS, selectors);
		// columnHeader = H2HeaderMap.get(columnHeader);
		columnHeader = this.getValueForUniqueName(columnHeader);
		return Arrays.asList(builder.getColumn(columnHeader, true)).iterator();
	}

	@Override
	public void save(String fileName) {
		String fileNameBase = fileName.substring(0, fileName.lastIndexOf("."));
		this.metaData.save(fileNameBase);
		if(fileName != null && !fileName.isEmpty() && getH2Headers() != null) {
			Properties props = builder.save(fileName, getH2Headers());
			
			OutputStream output = null;
			try {
				output = new FileOutputStream(fileNameBase+"_PROP.properties");
				props.store(output, null);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if(output != null) {
						output.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
	}

	/**
	 * Open a serialized TinkerFrame This is used with in InsightCache class
	 * 
	 * @param fileName
	 *            The file location to the cached graph
	 * @param userId
	 *            The userId who is creating this instance of the frame
	 * @return
	 */
	public H2Frame open(String fileName, String userId) {
		// create the new H2Frame instance
		H2Frame h2Frame = new H2Frame();
		// set the user id who invoked this new instance
		// this also sets the correct schema for the in memory connection
		h2Frame.setUserId(userId);
		// the builder is responsible for loading in the actual serialized
		// values
		// the set user id is responsible for setting the correct schema inside
		// the builder object
		
		String fileNameBase = fileName.substring(0, fileName.lastIndexOf("."));
		Properties prop = new Properties();
		try {
			prop.load(new BufferedReader(new FileReader(fileNameBase+"_PROP.properties")));
			h2Frame.builder.open(fileName, prop);
		} catch (FileNotFoundException e) {
			//need these here so legacy caches will still work, will transition this out as people's caches are deleted and recreated
			h2Frame.builder.open(fileName, prop);
		} catch (IOException e) {
			h2Frame.builder.open(fileName, prop);
		}
		
		// need to also set the metaData
		// the meta data fileName parameter passed is going to be the same as
		// the name as the file of the actual instances
		// this isn't the actual fileName of the file, the metadata appends the
		// predefined prefix for the file
		h2Frame.metaData.open(fileNameBase);
		List<String> primKeys = h2Frame.metaData.getPrimKeys();
		if (primKeys.size() == 1) {
			h2Frame.metaData.setVertexValue(primKeys.get(0),
					h2Frame.builder.tableName);
		}
		// set the list of headers in the class variable
		List<String> fullNames = h2Frame.metaData.getColumnNames();
		h2Frame.headerNames = fullNames.toArray(new String[fullNames.size()]);

		// return the new instance
		return h2Frame;
	}

	public List<String> getSelectors() {
		if (headerNames == null)
			return new ArrayList<String>();
		List<String> selectors = new ArrayList<String>();
		for (int i = 0; i < headerNames.length; i++) {
			if (!columnsToSkip.contains(headerNames[i])) {
				selectors.add(headerNames[i]);
			}
		}
		return selectors;
	}

	public List<String> getH2Selectors() {
		List<String> selectors = getSelectors();
		List<String> h2selectors = new ArrayList<>(selectors.size());
		for (int i = 0; i < selectors.size(); i++) {
			h2selectors.add(getH2Header(selectors.get(i)));
		}
		return h2selectors;
	}

	private String[] getH2Headers() {
		if (headerNames == null)
			return null;
		String[] h2Headers = new String[headerNames.length];
		for (int i = 0; i < headerNames.length; i++) {
			h2Headers[i] = getH2Header(headerNames[i]);
		}
		return h2Headers;
	}

	private String getH2Header(String uniqueName) {
			return this.getValueForUniqueName(uniqueName);
	}

	protected void setH2Headers(Map<String, String> headers) {
		this.joinHeaders = headers;
	}

	// private String[] getH2Types() {
	// if(headerNames == null) return null;
	// String[] h2Types = new String[headerNames.length];
	// for(int i = 0; i < headerNames.length; i++) {
	// h2Types[i] = this.metaData.getDBDataType(headerNames[i]);
	// }
	// return h2Types;
	// }

	private Map<String, String> getH2HeadersAndTypes() {

		if (headerNames == null)
			return null;
		Map<String, String> retMap = new HashMap<String, String>(
				headerNames.length);
		for (String header : headerNames) {
			String h2Header = this.getH2Header(header);
			String h2Type = Utility.convertDataTypeToString(this.metaData
					.getDataType(header));
			retMap.put(h2Header, h2Type);
		}
		return retMap;
	}

	// relationships in the form tableA.columnA.tableB.columnB
	public void setRelations(List<String> relations) {
		// use this to set the relationships gathered from the xl file helper
		Map<String, Set<String>> edgeHash = new HashMap<>();
		for (String relation : relations) {
			String[] relationComps = relation.split(".");
			String tableA = relationComps[0];
			String columnA = relationComps[1];
			String tableB = relationComps[2];
			String columnB = relationComps[3];

			String key = tableA + "__" + columnA;
			Set<String> valueSet = new HashSet<String>();
			String value = tableB + "__" + columnB;
			valueSet.add(value);

			if (edgeHash.containsKey(key) && edgeHash.get(key) != null) {
				edgeHash.get(key).addAll(valueSet);
			} else {
				edgeHash.put(key, valueSet);
			}
		}

		TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash,
				getNode2ValueHash(edgeHash));
	}

	protected String getCleanHeader(String metaNodeName) {
		String metaNodeValue;
		if (metaNodeName.equals(TinkerFrame.PRIM_KEY)) {
			metaNodeValue = builder.getNewTableName();
		} else {
			metaNodeValue = H2Builder.cleanHeader(metaNodeName);
		}

		return metaNodeValue;
	}

	private Map<String, String> getNode2ValueHash(Map<String, Set<String>> edgeHash) {
		Set<String> masterSet = new HashSet<String>();
		masterSet.addAll(edgeHash.keySet());
		Collection<Set<String>> valSet = edgeHash.values();
		for (Set<String> val : valSet) {
			masterSet.addAll(val);
		}
		Map<String, String> trans = new HashMap<String, String>();
		for (String name : masterSet) {
			if (name.startsWith(TinkerFrame.PRIM_KEY)) {
				trans.put(name, builder.getNewTableName());
			} else {
				trans.put(name, getCleanHeader(name));
			}
		}
		return trans;
	}

	@Override
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine,	Vector<Map<String, String>> joinCols) {
		// process the meta data
		Map[] ret = super.mergeQSEdgeHash(edgeHash, engine, joinCols);

		// its a bit inefficient to loop through all the headers...
		// but this is better than looping through the edge hash
		// at least the logic in the the builder won't re-add columns

		// need to get the types for each of the names
		String[] types = new String[this.headerNames.length];
		// grab all the types for each header from the metadata
		Map<String, IMetaData.DATA_TYPES> typeMap = this.metaData.getColumnTypes();
		for (int i = 0; i < types.length; i++) {
			// convert the type to string
			types[i] = Utility.convertDataTypeToString(typeMap.get(this.headerNames[i]));
		}

		// alter the table
		builder.alterTableNewColumns(builder.tableName, this.headerNames, types);

		return ret;
	}

	@Override
	public void mergeEdgeHash(Map<String, Set<String>> edgeHash, Map<String, String> dataTypeMap) {
		// merge results with the tinker meta data and store data types'
		super.mergeEdgeHash(edgeHash, getNode2ValueHash(edgeHash), dataTypeMap);
		// now we need to create and/or modify the existing table to ensure it
		// has all the necessary columns

		// create a map of column to data type
		String[] cleanHeaders = new String[this.headerNames.length];
		String[] types = new String[this.headerNames.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = Utility.convertDataTypeToString(this.metaData.getDataType(this.headerNames[i]));
			cleanHeaders[i] = this.getValueForUniqueName(this.headerNames[i]);
		}

		if (builder.tableName == null) {
			builder.tableName = getTableNameForUniqueColumn(this.headerNames[0]);
		}
		builder.alterTableNewColumns(builder.tableName, cleanHeaders, types);
	}

	@Override
	public void join(ITableDataFrame table, String colNameInTable, String colNameInJoiningTable, double confidenceThreshold, IMatcher routine) {
		LOGGER.error("join method has not been implemented for H2Frame");
	}

	public String getValueForUniqueName(String name) {
		return this.metaData.getValueForUniqueName(name);
	}

	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
		addRelationship(rowCleanData);
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality,Map<String, String> logicalToValMap) {
		for (int i = 0; i < headers.length; i++) {
			headers[i] = H2Builder.cleanHeader(headers[i]);
		}

		String[] currHeaders = getH2Headers();

		for (int i = 0; i < headers.length - 1; i++) {
			String header1 = headers[i];
			Object value1 = values[i];
			for (int j = i + 1; j < headers.length; j++) {
				String header2 = headers[j];
				Object value2 = values[j];

				int index1 = ArrayUtilityMethods.arrayContainsValueAtIndex(currHeaders, header1);
				int index2 = ArrayUtilityMethods.arrayContainsValueAtIndex(currHeaders, header2);

				if (index2 < index1) {
					headers[i] = header2;
					values[i] = value2;

					headers[j] = header1;
					values[j] = value1;
				}
			}
		}

		builder.updateTable(currHeaders, values, headers);
		if(this.isJoined()) {
			builder.joiner.refreshView(this, builder.getViewTableName());
		}
	}

	/**
	 * Provides a HashMap containing metadata of the db connection: username, tableName, and schema.
	 * @return HashMap of database metadata.
	 * @throws SQLException Could not access H2Builder connection.
	 */
	public HashMap<String, String> getDatabaseMetaData() throws SQLException {
		HashMap<String, String> dbmdMap = new HashMap<String, String>();
		DatabaseMetaData dbmd = builder.conn.getMetaData();
		dbmdMap.put("username", dbmd.getUserName());
		dbmdMap.put("tableName", builder.getTableName());
		dbmdMap.put("schema", builder.getSchema());
		return dbmdMap;
	}

	@Override
	public void removeRelationship(Map<String, Object> cleanRow) {
		Set<String> columnNames = cleanRow.keySet();
		String[] columns = new String[columnNames.size()];
		String[] values = new String[columnNames.size()];
		int i = 0;
		for (String column : cleanRow.keySet()) {
			String col = this.getValueForUniqueName(column);
			Object value = cleanRow.get(col);
			String val = Utility.cleanString(value.toString(), true, true,
					false);
			columns[i] = col;
			values[i] = val;
			i++;
		}
		builder.deleteRow(columns, values);
	}

	@Override
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
		reactorNames.put(PKQLEnum.MATH_PARAM, "prerna.sablecc.MathParamReactor");
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.API, "prerna.sablecc.ApiReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.ColAddReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.H2ImportDataReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.OPEN_DATA, "prerna.sablecc.OpenDataReactor");
		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.ColSplitReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.ColSplitReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
		// switch(reactorType) {
		// case IMPORT_DATA : return new H2ImportDataReactor();
		// case COL_ADD : return new ColAddReactor();
		// }

		return reactorNames;
	}

	public void processIterator(Iterator<IHeadersDataRow> iterator,	String[] newHeaders, Map<String, String> logicalToValue, List<Map<String, String>> joins, String joinType) {

		// convert the new headers into value headers
		String[] valueHeaders = new String[newHeaders.length];
		if (logicalToValue == null) {
			for (int i = 0; i < newHeaders.length; i++) {
				valueHeaders[i] = this.getValueForUniqueName(newHeaders[i]);
			}
		} else {
			for (int i = 0; i < newHeaders.length; i++) {
				valueHeaders[i] = logicalToValue.get(newHeaders[i]);
			}
		}

		String[] types = new String[newHeaders.length];
		for (int i = 0; i < newHeaders.length; i++) {
			types[i] = Utility.convertDataTypeToString(this.metaData.getDataType(newHeaders[i]));
		}

		String[] columnHeaders = getColumnHeaders();

		// my understanding
		// need to get the list of columns that are currently inside the frame
		// this is because mergeEdgeHash has already occured and added the
		// headers into the metadata
		// thus, columnHeaders has both the old headers and the new ones that we
		// want to add
		// thus, go through and only keep the list of headers that are not in
		// the new ones
		// but also need to add those that are in the joinCols in case 2 headers
		// match
		List<String> adjustedColHeadersList = new Vector<String>();
		for (String header : columnHeaders) {
			if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(newHeaders,header)) {
				adjustedColHeadersList.add(this.getValueForUniqueName(header));
			} else {
				joinLoop: for (Map<String, String> join : joins) {
					if (join.keySet().contains(header)) {
						adjustedColHeadersList.add(this.getValueForUniqueName(header));
						break joinLoop;
					}
				}
			}
		}
		String[] adjustedColHeaders = adjustedColHeadersList.toArray(new String[] {});

		// get the join type
		Join jType = Join.INNER;
		if (joinType != null) {
			if (joinType.toUpperCase().startsWith("INNER")) {
				jType = Join.INNER;
			} else if (joinType.toUpperCase().startsWith("OUTER")) {
				jType = Join.FULL_OUTER;
			} else if (joinType.toUpperCase().startsWith("LEFT")) {
				jType = Join.LEFT_OUTER;
			} else if (joinType.toUpperCase().startsWith("RIGHT")) {
				jType = Join.RIGHT_OUTER;

				// due to stupid legacy code using partial
			} else if (joinType.toUpperCase().startsWith("PARTIAL")) {
				jType = Join.LEFT_OUTER;
			}
		}

		this.builder.processIterator(iterator, adjustedColHeaders,valueHeaders, types, jType);

		if(this.isJoined()) {
			builder.joiner.refreshView(this, builder.getViewTableName());
		}
		
	}

	/**
	 * Getter for RRunner. Instantiates a new RRunner if none exists based off current database metadata.
	 * @return RRunner
	 * @throws RserveException Most likely RServe was not started in local R
	 * @throws SQLException Could not access H2Builder data
	 */
	public RRunner getRRunner() throws RserveException, SQLException {
		if (this.r == null) {
			this.r = new RRunner(this.getDatabaseMetaData());
		}

		return this.r;
	}

	/**
	 * Closes out RConnection and stops server connection to H2Frame
	 */
	public void closeRRunner() {
		if (r != null) {
			this.r.close();
		}
	}

	public void dropTable() {
		if(this.isJoined()) {
			builder.joiner.unJoinFrame(this);
			builder.joiner = null;
		}
		this.builder.dropTable();
	}

	@Override
	public String getDataMakerName() {
		// could we just do "return this.class.toString();" ?
		return "H2Frame";
	}

	public boolean isJoined() {
		return builder.getJoinMode();
	}

	protected void setJoin(String viewTable) {
		builder.setView(viewTable);
	}

	protected void unJoin() {
		builder.unJoin();
	}

	public void openBackDoor() {
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine();				
			}
		};
		thread.start();
	}

	@Override
	/**
	 * Used to update the data id when data has changed within the frame
	 */
	public void updateDataId() {
		if(this.isJoined()) {
			builder.joiner.updateDataId(this.builder.getViewTableName());
		} else {
			updateDataId(1);
		}
	}

	protected void updateDataId(int val) {
		this.dataId = this.dataId.add(BigInteger.valueOf(val));
	}
	
//	public Map<? extends String, ? extends Object> getGraphOutput() {
//		//possibly store a graph structure
//		//create it lazily
//		Map<? extends String, ? extends Object> graphOutput;
//		if(graphCache == null) {
//			graphCache = TableDataFrameFactory.convertToTinkerFrameForGraph(this);
//		} else {
//			//filter?
//		}
//		graphOutput = graphCache.getGraphOutput();
//		//unfilter the graph?
////		graphCache.unfilter();
//		return graphOutput;
//	}
	
	/**
	 * Method printAllRelationship.
	 */
	private void openCommandLine() {
		LOGGER.warn("<<<<");
		String end = "";

		while(!end.equalsIgnoreCase("end")) {
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				LOGGER.info("Enter SQL");
				String query2 = reader.readLine();   
				if(query2!=null){
					long start = System.currentTimeMillis();
					end = query2;
					LOGGER.info("SQL is " + query2);

					ResultSet set = this.builder.runBackDoorQuery(query2);
					while(set != null && set.next()) {

						long time2 = System.currentTimeMillis();
						LOGGER.warn("time to execute : " + (time2 - start )+ " ms");
					}
				}
			} 
			catch (RuntimeException e) {e.printStackTrace();} 
			catch (IOException e) {e.printStackTrace();} 
			catch (SQLException e) {e.printStackTrace();}
		}
	}
}