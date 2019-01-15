package prerna.ds.h2;

import java.io.File;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.cache.CachePropFileFrameObject;
import prerna.cache.ICache;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.QueryStruct;
import prerna.ds.shared.AbstractTableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.interpreters.sql.H2SqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.RelationSet;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.rdf.engine.wrappers.RawRDBMSSelectWrapper;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;
import prerna.ui.components.playsheets.datamakers.JoinTransformation;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class H2Frame extends AbstractTableDataFrame {

	public static final String DATA_MAKER_NAME = "H2Frame";
	
	protected H2Builder builder;

	public H2Frame() {
		setSchema();
	}
	
	public H2Frame(String tableName) {
		setSchema();
		if(tableName != null && !tableName.isEmpty()) {
			this.builder.tableName = tableName;
		} else {
			this.builder.tableName = this.builder.getNewTableName();
		}
		setName(this.builder.tableName);
	}

	public H2Frame(String[] headers) {
		setSchema();
		int numHeaders = headers.length;
		String[] types = new String[numHeaders];
		for(int i = 0; i < numHeaders; i++) {
			types[i] = "STRING";
		}
		ImportUtility.parseHeadersAndTypeIntoMeta(this, headers, types, this.builder.getTableName());
		syncHeaders();
		builder.alterTableNewColumns(builder.tableName, headers, types);
	}

	public H2Frame(String[] headers, String[] types) {
		this("");
		ImportUtility.parseHeadersAndTypeIntoMeta(this, headers, types, this.builder.getTableName());
		syncHeaders();
		builder.alterTableNewColumns(builder.tableName, headers, types);
	}

	public H2Builder getBuilder(){
		return this.builder;
	}

	private void setSchema() {
		if (this.builder == null) {
			this.builder = new H2Builder();
			setName(this.builder.tableName);
		}
		this.builder.setSchema(this.userId +"_"+ Utility.getRandomString(10));
		// we also set the logger into the builder
		this.builder.setLogger(this.logger);
		// NOTE : WE ARE ALWAYS DOING ON-DISK!!!!
		convertToOnDiskFrame(null);
	}

	/**
	 * Setting the user id in the builder will automatically update the schema 
	 */
	public void setUserId(String userId) {
		super.setUserId(userId);
		this.setSchema();
	}
	
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
		this.builder.setLogger(logger);
	}

	public String getSchema() {
		return this.builder.getSchema();
	}

	public boolean isInMem() {
		return this.builder.isInMem();
	}
	
	/*************************** AGGREGATION METHODS *************************/

	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, Map<String, SemossDataType> typesMap) {
		addRowsViaIterator(it, this.builder.getTableName(), typesMap);
	}
	
	public void addRowsViaIterator(Iterator<IHeadersDataRow> it, String tableName, Map<String, SemossDataType> types) {
		long start = System.currentTimeMillis();
		logger.info("Begin adding new rows into table = " + getName());
		this.builder.addRowsViaIterator(it, tableName, types);
		long end = System.currentTimeMillis();
		logger.info("Done adding new rows in " + (end-start) +  "ms");
	}

	@Override
	public void addRow(Object[] cells, String[] headers) {
		String[] types = new String[headers.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = this.metaData.getHeaderTypeAsString(headers[i], this.builder.getTableName());
			cells[i] = cells[i] + "";
		}
		String[] stringArray = Arrays.copyOf(cells, cells.length, String[].class);

		// get table for headers
		this.addRow(builder.tableName, stringArray, headers, types);
	}

	// need to make this private if we are going with single table h2
	public void addRow(String tableName, String[] cells, String[] headers, String[] types) {
		this.builder.tableName = tableName;
		this.builder.addRow(tableName, cells, headers, types);
	}

	/**
	 * Create a prepared statement to efficiently update columns in a frame
	 * @param TABLE_NAME
	 * @param columnsToUpdate
	 * @param whereColumns
	 * @return
	 */
	public PreparedStatement createUpdatePreparedStatement(final String[] columnsToUpdate, final String[] whereColumns) {
		return this.builder.createUpdatePreparedStatement(this.builder.tableName, columnsToUpdate, whereColumns);
	}
	
	/**
	 * Create a prepared statement to efficiently insert new rows in a frame
	 * @param columns
	 * @return
	 */
	public PreparedStatement createInsertPreparedStatement(final String[] columns) {
		return this.builder.createInsertPreparedStatement(this.builder.tableName, columns);
	}

	
	/************************** END AGGREGATION METHODS **********************/
	
	public void convertToOnDiskFrame(String schema) {
		String previousPhysicalSchema = null;
		if(!isInMem()) {
			previousPhysicalSchema = getSchema();
		}
		
		// if null is passed in
		// we automatically create a new schema
		this.builder.convertFromInMemToPhysical(schema);
		
		// if it was already an existing physical schema
		// should delete the folder from the server
		if(previousPhysicalSchema != null) {
			File file = new File(previousPhysicalSchema);
			if(file.exists()) {
				String folder = file.getParent();
				logger.info("DELETING ON-DISK SCHEMA AT FOLDER PATH = " + folder);
				ICache.deleteFolder(folder);
			}
		}
	}
	
	@Override
	public IRawSelectWrapper query(String query) {
		logger.info("Executing query...");
		long start = System.currentTimeMillis();
		RawRDBMSSelectWrapper it = new RawRDBMSSelectWrapper();
		it.directExecutionViaConnection(this.builder.getConnection(), query, false);
		long end = System.currentTimeMillis();
		logger.info("Time to execute query on frame = " + (end-start) + "ms");
		return it;
	}
	
	@Override
	public IRawSelectWrapper query(SelectQueryStruct qs) {
		logger.info("Generating SQL query...");
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, this.metaData);
		H2SqlInterpreter interp = new H2SqlInterpreter(this);
		interp.setQueryStruct(qs);
		interp.setLogger(this.logger);
		String iteratorQuery = interp.composeQuery();
		logger.info("Done generating SQL query");
		return query(iteratorQuery);
	}

	@Override
	public void removeColumn(String columnHeader) {
		if (!ArrayUtilityMethods.arrayContainsValue(this.qsNames, columnHeader)) {
			return;
		}

		String tableName = this.builder.getTableName();
		this.builder.dropColumn(columnHeader);
		this.metaData.dropProperty(tableName + "__" + columnHeader, tableName);
		syncHeaders();
	}
	
	@Override
	public long size(String tableName) {
		if(this.builder.isEmpty(tableName)) {
			return 0;
		}
		return this.builder.getNumRecords(getName());
	}

	@Override
	public CachePropFileFrameObject save(String folderDir) throws IOException {
		CachePropFileFrameObject cf = new CachePropFileFrameObject();
		
		String frameName = this.getName();
		cf.setFrameName(frameName);
		
		//save frame
		String frameFileName = folderDir + DIR_SEPARATOR + frameName + ".gz";
		this.builder.save(frameFileName, frameName);
		cf.setFrameCacheLocation(frameFileName);

		// also save the meta details
		this.saveMeta(cf, folderDir, frameName);
		return cf;
	}

	/**
	 * Open a serialized H2Frame This is used with in InsightCache class
	 * 
	 * @param fileName
	 *            The file location to the cached graph
	 * @param userId
	 *            The userId who is creating this instance of the frame
	 * @return
	 */
	public void open(CachePropFileFrameObject cf) throws IOException {		
		//set the frame name to that of the cached frame name
		this.builder.tableName = cf.getFrameName();
		// load the frame
		this.builder.open(cf.getFrameCacheLocation());
		// open the meta details
		this.openCacheMeta(cf);
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
	public boolean isEmpty() {
		return this.builder.isEmpty(builder.tableName);
	}
	
	@Override
	public String getDataMakerName() {
		return H2Frame.DATA_MAKER_NAME;
	}

	/**
	 * Get the table name for the current frame
	 * @return
	 */
	@Override
	public String getName() {
		return this.builder.getTableName();
	}
	
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
	
	/**
	 * Execute a query and returns the ResultSet
	 * Responsibility of user to close the ResultSet
	 * @param query			The query to execute on the frame
	 * @return				ResultSet for the query
	 */
	public ResultSet execQuery(String query) {
		// execute a query and get back its result set
		return this.builder.executeQuery(query);
	}
	
	/**
	 * Return the set of columns which already have an index
	 * @return
	 */
	public Set<String> getColumnsWithIndexes() {
		Set<String> cols = new HashSet<String>();
		for(String tableColKey : this.builder.columnIndexMap.keySet()) {
			// table name and col name are appended together with +++
			cols.add(tableColKey.split("\\+\\+\\+")[1]);
		}
		return cols;
	}
	
	/**
	 * Add an index on a column
	 * @param columnName
	 */
	public void addColumnIndex(String columnName) {
		if(columnName.contains("__")) {
			String[] split = columnName.split("__");
			this.builder.addColumnIndex(split[0], split[1]);
		} else {
			String tableName = getName();
			this.builder.addColumnIndex(tableName, columnName);
		}
	}
	
	/**
	 * Add a multi column index
	 * @param columnName
	 */
	public void addColumnIndex(String[] columnName) {
		String tableName = getName();
		this.builder.addColumnIndex(tableName, columnName);
	}
	
	/**
	 * Remove an index on a column
	 * @param columnName
	 */
	public void removeColumnIndex(String columnName) {
		String tableName = getName();
		this.builder.removeColumnIndex(tableName, columnName);
	}
	
	/**
	 * Remove a multi column index
	 * @param columnName
	 */
	public void removeColumnIndex(String[] columnName) {
		String tableName = getName();
		this.builder.removeColumnIndex(tableName, columnName);
	}

	public void deleteAllRows() {
		String tableName = getName();
		this.builder.deleteAllRows(tableName);
	}
	
	/**
	 * Adds a new empty column to the frame and adds the metadata
	 * @param newHeaders 
	 * @param types
	 * @param tableName
	 */
	public void addNewColumn(String[] newHeaders, String[] types, String tableName) {
		this.builder.alterTableNewColumns(tableName, newHeaders, types);
		OwlTemporalEngineMeta meta = this.getMetaData();
		for(int i = 0; i < newHeaders.length; i++) {
			meta.addProperty(tableName, tableName + "__" + newHeaders[i]);
			meta.setAliasToProperty(tableName + "__" + newHeaders[i], newHeaders[i]);
			meta.setDataTypeToProperty(tableName + "__" + newHeaders[i], types[i]);
		}
	}
	
	@Override
	public void close() {
		super.close();
		this.builder.dropTable();
		if(this.builder.server != null) {
			this.builder.server.shutdown();
		}
		if(!isInMem()) {
			dropOnDiskTemporalSchema();
		}
	}
	
	private void dropOnDiskTemporalSchema() {
		if(!isInMem()) {
			this.builder.closeConnection();
			String schema = getSchema();
			File file = new File(schema);
			String folder = file.getParent();
			logger.info("DELETING ON-DISK SCHEMA AT FOLDER PATH = " + folder);
			ICache.deleteFolder(folder);
		}
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * Deprecated DataMakerComponent stuff
	 */	
	
	@Override
	@Deprecated
	public Map<String, String> getScriptReactors() {
		Map<String, String> reactorNames = super.getScriptReactors();
		reactorNames.put(PKQLEnum.EXPR_TERM, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLEnum.EXPR_SCRIPT, "prerna.sablecc.ExprReactor");
		reactorNames.put(PKQLReactor.MATH_FUN.toString(),"prerna.sablecc.MathReactor");
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
		reactorNames.put(PKQLEnum.DATA_FRAME_CHANGE_TYPE, "prerna.sablecc.H2ChangeTypeReactor");
		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.H2VizReactor");
//		reactorNames.put(PKQLEnum.VIZ, "prerna.sablecc.VizReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_SET_EDGE_HASH, "prerna.sablecc.FlatTableSetEdgeHash");

		// rdbms connection logic
		reactorNames.put(PKQLEnum.DASHBOARD_JOIN, "prerna.sablecc.DashboardJoinReactor");
		reactorNames.put(PKQLEnum.NETWORK_CONNECT, "prerna.sablecc.ConnectReactor");
		reactorNames.put(PKQLEnum.NETWORK_DISCONNECT, "prerna.sablecc.DisConnectReactor");
		reactorNames.put(PKQLEnum.DATA_FRAME_DUPLICATES, "prerna.sablecc.H2DuplicatesReactor");
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
		reactorNames.put(PKQLEnum.EXCEL_API, "prerna.sablecc.ExcelApiReactor");
		reactorNames.put(PKQLEnum.WEB_API, "prerna.sablecc.WebApiReactor");
		reactorNames.put(PKQLEnum.FRAME_API, "prerna.sablecc.H2ApiReactor");
		reactorNames.put(PKQLEnum.FRAME_RAW_API, "prerna.sablecc.H2RawQueryApiReactor");

		reactorNames.put(PKQLEnum.CLEAR_DATA, "prerna.sablecc.H2ClearDataReactor");
		
		return reactorNames;
	}
	
	@Override
	@Deprecated
	public void processDataMakerComponent(DataMakerComponent component) {
		long startTime = System.currentTimeMillis();
		logger.info("Processing Component..................................");

		List<ISEMOSSTransformation> preTrans = component.getPreTrans();
		Vector<Map<String, String>> joinColList = new Vector<Map<String, String>>();
		String joinType = null;
		List<prerna.sablecc2.om.Join> joins = new ArrayList<prerna.sablecc2.om.Join>();
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
				prerna.sablecc2.om.Join colJoin = new prerna.sablecc2.om.Join(this.getName()+"__"+joinCol1, joinType, joinCol2);
				joins.add(colJoin);
				joinColList.add(joinMap);
			}
		}

		// logic to flush out qs -> qs2
		QueryStruct qs = component.getQueryStruct();
		// the component will either have a qs or a query string, account for that here
		SelectQueryStruct qs2 = null;
		if (qs == null) {
			String query = component.getQuery();
			qs2 = new HardSelectQueryStruct();
			((HardSelectQueryStruct) qs2).setQuery(query);
			qs2.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		} else {
			qs2 = new SelectQueryStruct();
			// add selectors
			Map<String, List<String>> qsSelectors = qs.getSelectors();
			for (String key : qsSelectors.keySet()) {
				for (String prop : qsSelectors.get(key)) {
					qs2.addSelector(key, prop);
				}
			}
			Set<String[]> rels = new RelationSet();
			Map<String, Map<String, List>> curRels = qs.getRelations();
			for(String up : curRels.keySet()) {
				Map<String, List> innerMap = curRels.get(up);
				for(String jType : innerMap.keySet()) {
					List downs = innerMap.get(jType);
					for(Object d : downs) {
						rels.add(new String[]{up, jType, d.toString()});
					}
				}
			}
			qs2.mergeRelations(rels);
			qs2.setQsType(QUERY_STRUCT_TYPE.ENGINE);
		}

		long time1 = System.currentTimeMillis();
		// set engine on qs2
		qs2.setEngineId(component.getEngineName());
		// instantiate h2importer with frame and qs
		IImporter importer = new H2Importer(this, qs2);
		if (joins.isEmpty()) {
			importer.insertData();
		} else {
			importer.mergeData(joins);
		}

		long time2 = System.currentTimeMillis();
		logger.info(" Processed Merging Data: " + (time2 - time1) + " ms");

		//
//      processPreTransformations(component, component.getPreTrans());
//      long time1 = System.currentTimeMillis();
//      LOGGER.info(" Processed Pretransformations: " + (time1 - startTime) + " ms");
//
//      IEngine engine = component.getEngine();
//      // automatically created the query if stored as metamodel
//      // fills the query with selected params if required
//      // params set in insightcreatrunner
//      String query = component.fillQuery();
//
//      String[] displayNames = null;
//      if (query.trim().toUpperCase().startsWith("CONSTRUCT")) {
//             // TinkerGraphDataModel tgdm = new TinkerGraphDataModel();
//             // tgdm.fillModel(query, engine, this);
//      } else if (!query.equals(Constants.EMPTY)) {
//             ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
//             String[] headers = wrapper.getDisplayVariables();
//             // if component has data from which we can construct a meta model
//             // then construct it and merge it
//             boolean hasMetaModel = component.getQueryStruct() != null;
//             if (hasMetaModel) {
//                   String[] startHeaders = getH2Headers();
//                   if (startHeaders == null) {
//                          startHeaders = new String[0];
//                   }
//                   Map<String, Set<String>> edgeHash = component.getQueryStruct().getReturnConnectionsHash();
//                   Map[] retMap = this.mergeQSEdgeHash(edgeHash, engine, joinColList, null);
//
//                   // set the addRow logic to false
//                   boolean addRow = false;
//                   // if all the headers are accounted or the frame is empty, then
//                   // the logic should only be inserting
//                   // the values from the iterator into the frame
//                   if (allHeadersAccounted(startHeaders, headers, joinColList) || this.isEmpty()) {
//                          addRow = true;
//                   }
//                   if (addRow) {
//                          while (wrapper.hasNext()) {
//                                IHeadersDataRow ss = (IHeadersDataRow) wrapper.next();
//                                addRow(ss.getValues(), headers);
//                          }
//                   } else {
//                          dmcProcessIterator(wrapper, wrapper.getDisplayVariables(), retMap[1], joinColList, joinType);
//                   }
//
////                 List<String> fullNames = this.metaData.getColumnNames();
//                   List<String> fullNames = this.newMetaData.getColumnNames();
//
//                   this.headerNames = fullNames.toArray(new String[fullNames.size()]);
//             }
//
//             // else default to primary key tinker graph
//             else {
//                   displayNames = wrapper.getDisplayVariables();
//                   Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(displayNames);
//                    TinkerMetaHelper.mergeEdgeHash(this.metaData, edgeHash, getNode2ValueHash(edgeHash));
//                   List<String> fullNames = this.metaData.getColumnNames();
//                   this.headerNames = fullNames.toArray(new String[fullNames.size()]);
//                   while (wrapper.hasNext()) {
//                          this.addRow(wrapper.next());
//                   }
//             }
//      }
//      // List<String> fullNames = this.metaData.getColumnNames();
//      // this.headerNames = fullNames.toArray(new String[fullNames.size()]);
//
//      long time2 = System.currentTimeMillis();
//      LOGGER.info(" Processed Wrapper: " + (time2 - time1) + " ms");
//
//      processPostTransformations(component, component.getPostTrans());
//      processActions(component, component.getActions());
//
//      long time4 = System.currentTimeMillis();
//      LOGGER.info("Component Processed: " + (time4 - startTime) + " ms");
  }

	@Deprecated
	public void dmcProcessIterator(Iterator<IHeadersDataRow> iterator,	String[] newHeaders, Map<String, String> logicalToValue, List<Map<String, String>> joins, String joinType) {
//		// convert the new headers into value headers
//		String[] valueHeaders = new String[newHeaders.length];
//		if (logicalToValue == null) {
//			for (int i = 0; i < newHeaders.length; i++) {
//				valueHeaders[i] = this.getValueForUniqueName(newHeaders[i]);
//			}
//		} else {
//			for (int i = 0; i < newHeaders.length; i++) {
//				valueHeaders[i] = logicalToValue.get(newHeaders[i]);
//			}
//		}
//
//		String[] types = new String[newHeaders.length];
//		for (int i = 0; i < newHeaders.length; i++) {
////			types[i] = Utility.convertDataTypeToString(this.metaData.getDataType(newHeaders[i]));
//			types[i] = this.newMetaData.getHeaderTypeAsString(newHeaders[i], this.builder.getTableName());
//		}
//
//		String[] columnHeaders = getColumnHeaders();
//
//		// my understanding
//		// need to get the list of columns that are currently inside the frame
//		// this is because mergeEdgeHash has already occured and added the
//		// headers into the metadata
//		// thus, columnHeaders has both the old headers and the new ones that we
//		// want to add
//		// thus, go through and only keep the list of headers that are not in
//		// the new ones
//		// but also need to add those that are in the joinCols in case 2 headers
//		// match
//		List<String> adjustedColHeadersList = new Vector<String>();
//		for (String header : columnHeaders) {
//			if (!ArrayUtilityMethods.arrayContainsValueIgnoreCase(newHeaders,header)) {
//				adjustedColHeadersList.add(this.getValueForUniqueName(header));
//			} else {
//				joinLoop: for (Map<String, String> join : joins) {
//					if (join.keySet().contains(header)) {
//						adjustedColHeadersList.add(this.getValueForUniqueName(header));
//						break joinLoop;
//					}
//				}
//			}
//		}
//		String[] adjustedColHeaders = adjustedColHeadersList.toArray(new String[] {});
//
//		// get the join type
//		Join jType = Join.INNER;
//		if (joinType != null) {
//			if (joinType.toUpperCase().startsWith("INNER")) {
//				jType = Join.INNER;
//			} else if (joinType.toUpperCase().startsWith("OUTER")) {
//				jType = Join.FULL_OUTER;
//			} else if (joinType.toUpperCase().startsWith("LEFT")) {
//				jType = Join.LEFT_OUTER;
//			} else if (joinType.toUpperCase().startsWith("RIGHT")) {
//				jType = Join.RIGHT_OUTER;
//
//				// due to stupid legacy code using partial
//			} else if (joinType.toUpperCase().startsWith("PARTIAL")) {
//				jType = Join.LEFT_OUTER;
//			}
//		}
//
//		this.builder.processIterator(iterator, adjustedColHeaders,valueHeaders, types, jType);
	}

//	/**
//	 * Determine if all the headers are taken into consideration within the
//	 * iterator This helps to determine if we need to perform an insert vs. an
//	 * update query to fill the frame
//	 * 
//	 * @param headers1
//	 *            The original set of headers in the frame
//	 * @param headers2
//	 *            The new set of headers from the iterator
//	 * @param joins
//	 *            Needs to take into consideration the joins since we can join
//	 *            on columns that do not have the same names
//	 * @return
//	 */
//	@Deprecated
//	private boolean allHeadersAccounted(String[] headers1, String[] headers2, List<Map<String, String>> joins) {
//		if (headers1.length != headers2.length) {
//			return false;
//		}
//
//		// add values to a set and compare
//		Set<String> header1Set = new HashSet<>();
//		Set<String> header2Set = new HashSet<>();
//
//		// make a set with headers1
//		for (String header : headers1) {
//			header1Set.add(header);
//		}
//
//		// make a set with headers2
//		for (String header : headers2) {
//			header2Set.add(header);
//		}
//
//		// add headers1 headers to headers2set if there is a matching join and
//		// remove the other header
//		for (Map<String, String> join : joins) {
//			for (String key : join.keySet()) {
//				header2Set.add(key);
//				header2Set.remove(join.get(key));
//			}
//		}
//
//		// take the difference
//		header2Set.removeAll(header1Set);
//
//		// return true if header sets matched, false otherwise
//		return header2Set.size() == 0;
//	}
//	
//	public void applyGroupBy(String[] column, String newColumnName, String valueColumn, String mathType) {
//		builder.processGroupBy(column, newColumnName, valueColumn, mathType, getColumnHeaders());
//	}
//	
//	public void mergeRowsViaIterator(Iterator<IHeadersDataRow> iterator, String[] newHeaders, String[] startingHeaders, String[] joinCols) {
//		int size = newHeaders.length;
//		SemossDataType[] types = new SemossDataType[size];
//		for (int i = 0; i < newHeaders.length; i++) {
//			types[i] = this.metaData.getHeaderTypeAsEnum(newHeaders[i], this.builder.getTableName());
//		}
//
//		try {
//			this.builder.mergeRowsViaIterator(iterator, newHeaders, types, startingHeaders, joinCols);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	
//	public void processIterator(Iterator<IHeadersDataRow> iterator,	String[] origHeaders, String[] newHeaders, String joinType) {
//		String tableName = this.builder.getTableName();
//		String[] types = new String[newHeaders.length];
//		for (int i = 0; i < newHeaders.length; i++) {
//			if(newHeaders[i].contains("__")) {
//				types[i] = this.metaData.getHeaderTypeAsString(newHeaders[i], tableName);
//			} else {
//				types[i] = this.metaData.getHeaderTypeAsString(tableName + "__" + newHeaders[i], tableName);
//			}
//		}
//		
//		// get the join type
//		Join jType = Join.INNER;
//		if (joinType != null) {
//			if (joinType.toUpperCase().startsWith("INNER")) {
//				jType = Join.INNER;
//			} else if (joinType.toUpperCase().startsWith("OUTER")) {
//				jType = Join.FULL_OUTER;
//			} else if (joinType.toUpperCase().startsWith("LEFT")) {
//				jType = Join.LEFT_OUTER;
//			} else if (joinType.toUpperCase().startsWith("RIGHT")) {
//				jType = Join.RIGHT_OUTER;
//				// due to stupid legacy code using partial
//			} else if (joinType.toUpperCase().startsWith("PARTIAL")) {
//				jType = Join.LEFT_OUTER;
//			}
//		}
//
//		// parameters are
//		// 1) iterator
//		// 2) original columns
//		// 3) new columns for iterator to create -> this can be different from the original iterators headers
//		// 			it will create the table with the correct ones that i want so merging the tables is easier
//		// 4) the types for the new table -> we get this from the meta... hopefully names match up :O
//		// 5) the join type to use
//		this.builder.processIterator(iterator, origHeaders, newHeaders, types, jType);
//		syncHeaders();
//	}
//	
//	
//	
//	
//	
	
	
}