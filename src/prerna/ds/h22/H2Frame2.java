package prerna.ds.h22;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.cache.ICache;
import prerna.ds.AbstractTableDataFrame;
import prerna.ds.RdbmsTableMetaData;
import prerna.ds.TinkerFrame;
import prerna.ds.TinkerMetaData;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.h22.H2Builder2.Join;
import prerna.ds.util.H2FilterHash;
import prerna.ds.util.RdbmsFrameUtility;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.r.RRunner;
import prerna.poi.main.RDBMSEngineCreationHelper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Constants;
import prerna.util.Utility;

public class H2Frame2 extends AbstractTableDataFrame {

	private static final Logger LOGGER = LogManager.getLogger(H2Frame2.class.getName());

	H2Builder2 builder;
	RdbmsTableMetaData tableMeta;

	// this was being used when we wanted the sql interpreter to create the
	// traverse query
	// IQueryInterpreter interp = new SQLInterpreter();
	RRunner r = null;

	public H2Frame2(String[] headers) {
		for(int i = 0; i < headers.length; i++) {
			headers[i] = RDBMSEngineCreationHelper.cleanTableName(headers[i]);
		}
		this.headerNames = headers;
		this.metaData = new TinkerMetaData();
		Map<String, Set<String>> primKeyEdge = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
		TinkerMetaHelper.mergeEdgeHash(this.metaData, primKeyEdge);
		setSchema();
		tableMeta = new RdbmsTableMetaData(builder.getConnection());
	}

	public H2Frame2() {
		this.metaData = new TinkerMetaData();
		setSchema();
	}
	
	public H2Frame2(IMetaData metaData) {
		this.metaData = metaData;
		setSchema();
		tableMeta = new RdbmsTableMetaData(builder.getConnection());
		String[] headers = this.getColumnHeaders();
		String[] types = new String[headers.length];
		for(int i = 0; i < headers.length; i++) {
			types[i] = Utility.convertDataTypeToString(getDataType(headers[i]));
		}
		builder.alterTableNewColumns(tableMeta.getTableName(), this.headerNames, types);
	}

	//added as a path to get connection url for current dataframe
	public H2Builder2 getBuilder(){
		return this.builder;
	}

	private void setSchema() {
		if (this.builder == null) {
			this.builder = new H2Builder2();
		}
		this.builder.setSchema(this.userId);
	}

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

		String[] types = new String[headers.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = Utility.convertDataTypeToString(this.metaData.getDataType(headers[i]));
			// need to stringify everything
			cells[i] = cells[i].toString();
		}
		String[] stringArray = Arrays.copyOf(cells, cells.length, String[].class);

		// get table for headers
		this.addRow(stringArray, headers, types);
	}

	// need to make this private if we are going with single table h2
	public void addRow(String[] cells, String[] headers, String[] types) {
		this.builder.addRow(cells, headers, types);
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
				String[] startHeaders = getColumnHeaders();//getH2Headers();
				if (startHeaders == null) {
					startHeaders = new String[0];
				}
				Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
				Map[] retMap = this.mergeQSEdgeHash(edgeHash, engine, joinColList, null);

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
			this.tableMeta.getFilters().setFilters(columnHeader, filterValues, "=");
		}
	}

	@Override
	public void filter(String columnHeader, Map<String, List<Object>> filterValues) {
		if(columnHeader == null || filterValues == null) return;

		H2FilterHash filterHash = this.tableMeta.getFilters();
		DATA_TYPES type = this.metaData.getDataType(columnHeader);
		boolean isOrdinal = type != null && (type == DATA_TYPES.DATE || type == DATA_TYPES.NUMBER);


		String[] comparators = filterValues.keySet().toArray(new String[]{});
		for(int i = 0; i < comparators.length; i++) {
			String comparator = comparators[i];
			boolean override = i == 0;
			List<Object> filters = filterValues.get(comparator);

			comparator = comparator.trim();
			if(comparator.equals("=")) {

				if(override) filterHash.setFilters(columnHeader, filters, comparator);
				else filterHash.addFilters(columnHeader, filters, comparator);

			} else if(comparator.equals("!=")) { 

				if(override) filterHash.setFilters(columnHeader, filters, comparator);
				else filterHash.addFilters(columnHeader, filters, comparator);

			} else if(comparator.equals("<") || comparator.equals(">") || comparator.equals("<=") || comparator.equals(">=")) {

				if(isOrdinal) {

					if(override) filterHash.setFilters(columnHeader, filters, comparator);
					else filterHash.addFilters(columnHeader, filters, comparator);

				} else {
					throw new IllegalArgumentException(columnHeader	+ " is not a numeric column, cannot use operator "	+ comparator);
				}

			} else {
				// comparator not recognized...do equal by default? or do
				// nothing? or throw error?
				return;
			}
			this.metaData.setFiltered(columnHeader, true);
		}
	}

	@Override
	public void unfilter(String columnHeader) {
		this.metaData.setFiltered(columnHeader, false);
		this.tableMeta.getFilters().removeFilter(columnHeader);
	}

	@Override
	public void unfilter() {
		this.tableMeta.getFilters().clearFilters();
	}

	public Map<String, Map<String, Double>> getMinMaxValues(String col) {
		Map<String, Map<String, Double>> minMaxValues = new HashMap<String, Map<String, Double>>();
		if(this.metaData.getDataType(col) == IMetaData.DATA_TYPES.NUMBER) {
			Map<String, Double> minMax = new HashMap<String, Double>();

			// sort unfiltered array to pull relative min and max of unfiltered data
			double min = getMin(col);
			double max = getMax(col);
			
			double absMin = builder.getStat(col, "MIN", true);
			double absMax = builder.getStat(col, "MAX", true);

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

			minMaxValues.put(col, minMax);
		}
		return minMaxValues;
	}


	/****************************** END FILTER METHODS ******************************************/

	@Override
	public Iterator<Object[]> iterator() {
		H2IteratorOptions options = new H2IteratorOptions();
		options.setSelectors(getSelectors());
		return iterator(options);
	}
	
	public Iterator<Object[]> iterator(H2IteratorOptions options) {
		options.setTableMeta(tableMeta);
		RdbmsIteratorBuilder builder = new RdbmsIteratorBuilder();
		return (Iterator<Object[]>)builder.buildIterator(options);
	}

	@Override
	public Iterator<Object> uniqueValueIterator(String columnHeader, boolean iterateAll) {
		
		H2IteratorOptions options = new H2IteratorOptions();
		options.setSelectors(Arrays.asList(columnHeader));
		options.withSingleColumn(true);
		options.setTableMeta(tableMeta);
		
		RdbmsIteratorBuilder builder = new RdbmsIteratorBuilder();
		return (Iterator<Object>)builder.buildIterator(options);
	}
	
	@Override
	public Object[] getColumn(String columnHeader) {
		Iterator<Object> iterator = uniqueValueIterator(columnHeader, false);
		List<Object> valuesList = new ArrayList<Object>();
		while(iterator.hasNext()) {
			valuesList.add(iterator.next());
		}
		return valuesList.toArray();
	}

	@Override
	public Double[] getColumnAsNumeric(String columnHeader) {
		if (isNumeric(columnHeader)) {
			Object[] array = getColumn(columnHeader);

			List<Double> numericCol = new ArrayList<Double>();
			for (Object row : array) {
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
	public Integer getUniqueInstanceCount(String columnHeader) {
		try {
			String query = "SELECT COUNT(DISTINCT "+columnHeader+") FROM " + this.tableMeta.getViewTableName();
			ResultSet rs = this.tableMeta.getConnection().createStatement().executeQuery(query);
			return rs.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Double getMin(String columnHeader) {
		// make sure its a number
		if (this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			return builder.getStat(columnHeader, "MIN", false);
		}
		return null;
	}

	@Override
	public Double getMax(String columnHeader) {
		if (this.metaData.getDataType(columnHeader).equals(IMetaData.DATA_TYPES.NUMBER)) {
			return builder.getStat(columnHeader, "MAX", false);
		}
		return null;
	}

	@Override
	public Iterator<List<Object[]>> scaledUniqueIterator(String columnHeader, Map<String, Object> options) {
		List<String> selectors = null;
		Double[] max = null;
		Double[] min = null;
		if (options != null && options.containsKey(AbstractTableDataFrame.SELECTORS)) {
			// get the max and min values based on the order that is defined
			selectors = (List<String>) options.get(AbstractTableDataFrame.SELECTORS);
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
			selectors = getSelectors();
			max = getMax();
			min = getMin();
		}

		Map<String, String> headerTypes = this.getH2HeadersAndTypes();

//		ScaledUniqueH2FrameIterator iterator = new ScaledUniqueH2FrameIterator(columnHeader, builder.tableName, builder, max, min, headerTypes, selectors);
//		return iterator;
		return null;
	}

	@Override
	public void addRelationship(Map<String, Object> cleanRow) {
		Object[] origValues = null;
		String[] origColumnHeaders = null;	
		boolean multiUpdates = false;
		
		Set<String> keySet = cleanRow.keySet();
		Map<String, Object> adjustedCleanRow = new LinkedHashMap<String, Object>();
		
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

		Arrays.sort(columnHeaders, new java.util.Comparator<String>() {

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
//			columnHeaders[i] = H2Builder2.cleanHeader(columnHeaders[i]);
		}		
		
		if(multiUpdates)			
			builder.updateTable2(origColumnHeaders, origValues, columnHeaders, values);		
		else
			builder.updateTable(getColumnHeaders(), values, columnHeaders);
	}

	@Override
	public void removeColumn(String columnHeader) {
		if (!ArrayUtilityMethods.arrayContainsValue(this.headerNames, columnHeader)) {
			return;
		}

		// drop the column from the h2 table
		builder.dropColumn(columnHeader);
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
	public void save(String fileName) {
		String fileNameBase = fileName.substring(0, fileName.lastIndexOf("."));
		this.metaData.save(fileNameBase);
		if(fileName != null && !fileName.isEmpty() && getColumnHeaders() != null) {
			Properties props = builder.save(fileName, getColumnHeaders());
			
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
	public H2Frame2 open(String fileName, String userId) {
		// create the new H2Frame instance
		H2Frame2 h2Frame = new H2Frame2();
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
			h2Frame.metaData.setVertexValue(primKeys.get(0), h2Frame.builder.tableMetaData.getTableName());
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
	
	private Map<String, String> getH2HeadersAndTypes() {

		if (headerNames == null)
			return null;
		Map<String, String> retMap = new HashMap<String, String>(
				headerNames.length);
		for (String header : headerNames) {
			String h2Header = header;
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
				trans.put(name, RdbmsFrameUtility.getNewTableName());
			} else {
				trans.put(name, name);
			}
		}
		return trans;
	}

	@Override
	public Map[] mergeQSEdgeHash(Map<String, Set<String>> edgeHash, IEngine engine,	Vector<Map<String, String>> joinCols, Map<String, Boolean> makeUniqueNameMap) {
		// process the meta data
		Map[] ret = super.mergeQSEdgeHash(edgeHash, engine, joinCols, makeUniqueNameMap);

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
		// CHANGE! we do not alter the table in this instance because whenever we go through mergeQSEdgeHash
		// the flow is determined to be adding new headers in method
//		builder.alterTableNewColumns(builder.tableName, this.headerNames, types);

		return ret;
	}

	@Override
	public void mergeEdgeHash(Map<String, Set<String>> edgeHash, Map<String, String> dataTypeMap) {
		// merge results with the tinker meta data and store data types'
		super.mergeEdgeHash(edgeHash, getNode2ValueHash(edgeHash), dataTypeMap);
		// now we need to create and/or modify the existing table to ensure it
		// has all the necessary columns

		// create a map of column to data type
		String[] types = new String[this.headerNames.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = Utility.convertDataTypeToString(this.metaData.getDataType(this.headerNames[i]));
		}

		builder.alterTableNewColumns(tableMeta.getTableName(), headerNames, types);
	}

	@Override
	public void addRelationship(String[] headers, Object[] values, Map<Integer, Set<Integer>> cardinality,Map<String, String> logicalToValMap) {
		String[] currHeaders = getColumnHeaders();

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
	}

	
	/**
	 * Provides a HashMap containing metadata of the db connection: username, tableName, and schema.
	 * @return HashMap of database metadata.
	 * @throws SQLException Could not access H2Builder connection.
	 */
	public HashMap<String, String> getDatabaseMetaData() throws SQLException {
		HashMap<String, String> dbmdMap = new HashMap<String, String>();
		DatabaseMetaData dbmd = builder.getBuilderMetadata();
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
			Object value = cleanRow.get(column);
			String val = Utility.cleanString(value.toString(), true, true,
					false);
			columns[i] = column;
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
		reactorNames.put(PKQLEnum.CSV_TABLE, "prerna.sablecc.CsvTableReactor");
		reactorNames.put(PKQLEnum.COL_CSV, "prerna.sablecc.ColCsvReactor"); // it almost feels like I need a way to tell when to do this and when not but let me see
		reactorNames.put(PKQLEnum.ROW_CSV, "prerna.sablecc.RowCsvReactor");
		reactorNames.put(PKQLEnum.PASTED_DATA, "prerna.sablecc.PastedDataReactor");
		reactorNames.put(PKQLEnum.WHERE, "prerna.sablecc.ColWhereReactor");
		reactorNames.put(PKQLEnum.REL_DEF, "prerna.sablecc.RelReactor");
		reactorNames.put(PKQLEnum.REMOVE_DATA, "prerna.sablecc.RemoveDataReactor");
		reactorNames.put(PKQLEnum.FILTER_DATA, "prerna.sablecc.ColFilterReactor");
		reactorNames.put(PKQLEnum.UNFILTER_DATA, "prerna.sablecc.ColUnfilterReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME, "prerna.sablecc.DataFrameReactor");
		reactorNames.put(PKQLEnum.DATA_TYPE, "prerna.sablecc.DataTypeReactor");
		reactorNames.put(PKQLEnum.DATA_CONNECT, "prerna.sablecc.DataConnectReactor");
		reactorNames.put(PKQLEnum.JAVA_OP, "prerna.sablecc.JavaReactorWrapper");
		
		// h2 specific reactors
		reactorNames.put(PKQLEnum.COL_ADD, "prerna.sablecc.H2ColAddReactor");
		reactorNames.put(PKQLEnum.COL_SPLIT, "prerna.sablecc.H2ColSplitReactor");
		reactorNames.put(PKQLEnum.IMPORT_DATA, "prerna.sablecc.H2ImportDataReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DuplicatesReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.H2VizReactor");
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");

		// rdbms connection logic
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DataFrameDuplicatesReactor");
		reactorNames.put(PKQLEnum.COL_FILTER_MODEL, "prerna.sablecc.H2ColFilterModelReactor");
		
		// h2 specific expression handlers		
		reactorNames.put(PKQLEnum.SUM, "prerna.sablecc.expressions.sql.SqlSumReactor");
		reactorNames.put(PKQLEnum.MAX, "prerna.sablecc.expressions.sql.SqlMaxReactor");
		reactorNames.put(PKQLEnum.MIN, "prerna.sablecc.expressions.sql.SqlMinReactor");
		reactorNames.put(PKQLEnum.AVERAGE, "prerna.sablecc.expressions.sql.SqlAverageReactor");
		reactorNames.put(PKQLEnum.COUNT, "prerna.sablecc.expressions.sql.SqlCountReactor");
		reactorNames.put(PKQLEnum.COUNT_DISTINCT, "prerna.sablecc.expressions.sql.SqlUniqueCountReactor");
		reactorNames.put(PKQLEnum.CONCAT, "prerna.sablecc.expressions.sql.SqlConcatReactor");
		reactorNames.put(PKQLEnum.GROUP_CONCAT, "prerna.sablecc.expressions.sql.SqlGroupConcatReactor");
		reactorNames.put(PKQLEnum.UNIQUE_GROUP_CONCAT, "prerna.sablecc.expressions.sql.SqlDistinctGroupConcatReactor");
		reactorNames.put(PKQLEnum.ABSOLUTE, "prerna.sablecc.expressions.sql.SqlAbsoluteReactor");
		reactorNames.put(PKQLEnum.ROUND, "prerna.sablecc.expressions.sql.SqlRoundReactor");
		reactorNames.put(PKQLEnum.COS, "prerna.sablecc.expressions.sql.SqlCosReactor");
		reactorNames.put(PKQLEnum.SIN, "prerna.sablecc.expressions.sql.SqlSinReactor");
		reactorNames.put(PKQLEnum.TAN, "prerna.sablecc.expressions.sql.SqlTanReactor");
		reactorNames.put(PKQLEnum.CEILING, "prerna.sablecc.expressions.sql.SqlCeilingReactor");
		reactorNames.put(PKQLEnum.FLOOR, "prerna.sablecc.expressions.sql.SqlFloorReactor");
		reactorNames.put(PKQLEnum.LOG, "prerna.sablecc.expressions.sql.SqlLogReactor");
		reactorNames.put(PKQLEnum.LOG10, "prerna.sablecc.expressions.sql.SqlLog10Reactor");
		reactorNames.put(PKQLEnum.SQRT, "prerna.sablecc.expressions.sql.SqlSqrtReactor");
		reactorNames.put(PKQLEnum.POWER, "prerna.sablecc.expressions.sql.SqlPowerReactor");
		reactorNames.put(PKQLEnum.CORRELATION_ALGORITHM, "prerna.ds.h2.H2CorrelationReactor");

		// default to sample stdev
		reactorNames.put(PKQLEnum.STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlSampleStandardDeviationReactor");
		reactorNames.put(PKQLEnum.SAMPLE_STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlSampleStandardDeviationReactor");
		reactorNames.put(PKQLEnum.POPULATION_STANDARD_DEVIATION, "prerna.sablecc.expressions.sql.H2SqlPopulationStandardDeviationReactor");
//		reactorNames.put(PKQLEnum.MEDIAN, "prerna.sablecc.expressions.sql.SqlMedianReactor");
		
		reactorNames.put(PKQLEnum.QUERY_API, "prerna.sablecc.QueryApiReactor");
		reactorNames.put(PKQLEnum.CSV_API, "prerna.sablecc.CsvApiReactor");
		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
		reactorNames.put(PKQLEnum.R_API, "prerna.sablecc.RApiReactor");
		
		reactorNames.put(PKQLEnum.CLEAR_DATA, "prerna.sablecc.H2ClearDataReactor");
		
		return reactorNames;
	}

	public void processIterator(Iterator<IHeadersDataRow> iterator,	String[] newHeaders, Map<String, String> logicalToValue, List<Map<String, String>> joins, String joinType) {

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
				adjustedColHeadersList.add(header);
			} else {
				joinLoop: for (Map<String, String> join : joins) {
					if (join.keySet().contains(header)) {
						adjustedColHeadersList.add(header);
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

		this.builder.processIterator(iterator, adjustedColHeaders, newHeaders, types, jType);		
	}
	
	@Override
	public boolean isEmpty() {
		return this.builder.isEmpty();
	}
	
	@Override
	public int getNumRows() {
		return this.builder.getNumRows();
	}
	
	public int getNumRecords() {
		return getNumRows() * getColumnHeaders().length;
	}

	public void dropTable() {
		this.builder.dropTable();
	}

	@Override
	public String getDataMakerName() {
		return this.getClass().getSimpleName();
	}

	@Override
	/**
	 * Used to update the data id when data has changed within the frame
	 */
	public void updateDataId() {
			updateDataId(1);
	}

	protected void updateDataId(int val) {
		this.dataId = this.dataId.add(BigInteger.valueOf(val));
	}

	public Map<String, Map<String, Set<Object>>> getFilterHash() {
		return this.tableMeta.getFilters().getFilterHash();
	}


		
	/******************************
	 * METHODS NO LONGER NECESSARY
	 ******************************/
	
	@Override
	public void addRelationship(Map<String, Object> rowCleanData, Map<String, Set<String>> edgeHash, Map<String, String> logicalToTypeMap) {
		addRelationship(rowCleanData);
	}

	@Override
	public Iterator<Object[]> iterator(Map<String, Object> options) {
		return null;
	}
	
	public Map<String, Object[]> getFilterTransformationValues() {
		Map<String, Object[]> retMap = new HashMap<String, Object[]>();
		return retMap;
	}

	@Override
	public Object[] getFilterModel() {
		return null;
	}
	
	//SHOULDN'T BE HERE...PUT IN SOME SORT OF HELPER CLASS
	/**
	 * Execute a query and returns the results in a matrix
	 * @param query			The query to execute on the frame
	 * @return				List<Object[]> of the query data
	 */
	public List<Object[]> getFlatTableFromQuery(String query) {
		// this is to execute a query and get all its results as a matrix
		// this is useful when you know the number of results are pretty small
		// nice because you do not need to handle the rs object directly
		return this.builder.getFlatTableFromQuery(query);
	}
	
//	// TODO : this won't with main column table
//	public String getTableNameForUniqueColumn(String uniqueName) {
//		return this.metaData.getParentValueOfUniqueNode(uniqueName);
//	}
	
//	/**
//	 * Create a prepared statement to efficiently update columns in a frame
//	 * @param TABLE_NAME
//	 * @param columnsToUpdate
//	 * @param whereColumns
//	 * @return
//	 */
//	public PreparedStatement createUpdatePreparedStatement(final String[] columnsToUpdate, final String[] whereColumns) {
//		return this.builder.createUpdatePreparedStatement(this.tableMeta.getTableName(), columnsToUpdate, whereColumns);
//	}
//	
//	/**
//	 * Create a prepared statement to efficiently insert new rows in a frame
//	 * @param columns
//	 * @return
//	 */
//	public PreparedStatement createInsertPreparedStatement(final String[] columns) {
//		return this.builder.createInsertPreparedStatement(this.tableMeta.getTableName(), columns);
//	}
	
//	public boolean isInMem() {
//	return this.tableMeta.isInMem();
//}

//public void convertToOnDiskFrame(String schema) {
//	String previousPhysicalSchema = null;
//	if(!isInMem()) {
//		previousPhysicalSchema = getSchema();
//	}
//	
//	// if null is passed in
//	// we automatically create a new schema
//	this.builder.convertFromInMemToPhysical(schema);
//	
//	// if it was already an existing physical schema
//	// should delete the folder from the server
//	if(previousPhysicalSchema != null) {
//		File file = new File(previousPhysicalSchema);
//		String folder = file.getParent();
//		LOGGER.info("DELETING ON-DISK SCHEMA AT FOLDER PATH = " + folder);
//		ICache.deleteFolder(folder);
//	}
//}
//
//public void dropOnDiskTemporalSchema() {
//	if(!isInMem()) {
//		this.builder.closeConnection();
//		String schema = getSchema();
//		File file = new File(schema);
//		String folder = file.getParent();
//		LOGGER.info("DELETING ON-DISK SCHEMA AT FOLDER PATH = " + folder);
//		ICache.deleteFolder(folder);
//	}
//}
//
//public String getSchema() {
//	return this.builder.getSchema();
//}
}