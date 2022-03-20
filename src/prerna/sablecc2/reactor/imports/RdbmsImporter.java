package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsImporter extends AbstractImporter {

	private static final Logger logger = LogManager.getLogger(RdbmsImporter.class);

	private AbstractRdbmsFrame dataframe;
	private AbstractSqlQueryUtil queryUtil;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;
	
	public RdbmsImporter(AbstractRdbmsFrame dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.queryUtil = dataframe.getQueryUtil();
		this.qs = qs;
		try {
			this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					new NounMetadata("Error occured executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
	}
	
	public RdbmsImporter(AbstractRdbmsFrame dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.queryUtil = dataframe.getQueryUtil();
		this.qs = qs;
		this.it = it;
		// generate the iterator
		if(this.it == null) {
			try {
				this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(
						new NounMetadata("Error occured executing query before loading into frame", 
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
	}

	@Override
	public void insertData() {
		ImportUtility.parseQueryStructToFlatTable(this.dataframe, this.qs, this.dataframe.getName(), this.it, false);
		processInsertData();
	}
	
	@Override
	public void insertData(OwlTemporalEngineMeta metaData) {
		this.dataframe.setMetaData(metaData);
		processInsertData();
	}
	
	/**
	 * Based on the metadata that was set (either through QS processing or directly passed in)
	 * Insert data from the iterator that the QS contains
	 */
	private void processInsertData() {
		// get the meta information from the new metadata
		Map<String, SemossDataType> rawDataTypeMap = this.dataframe.getMetaData().getHeaderToTypeMap();
		
		// TODO: this is annoying, need to get the frame on the same page as the meta
		Map<String, SemossDataType> dataTypeMap = new HashMap<String, SemossDataType>();
		for(String rawHeader : rawDataTypeMap.keySet()) {
			dataTypeMap.put(rawHeader.split("__")[1], rawDataTypeMap.get(rawHeader));
		}
		
		// use the base table name
		String tableName = this.dataframe.getName();
		try {
			this.dataframe.addRowsViaIterator(this.it, tableName, dataTypeMap);
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			// if we have an error
			// just make sure the headers are all there
			int size = dataTypeMap.size();
			String[] newHeaders = new String[size];
			String[] newTypes = new String[size];
			int counter = 0;
			for(String header : dataTypeMap.keySet()) {
				newHeaders[counter] = header;
				newTypes[counter] = this.dataframe.getQueryUtil().cleanType(dataTypeMap.get(header).toString());
				counter++;
			}
			try {
				this.dataframe.getBuilder().alterTableNewColumns(tableName, newHeaders, newTypes);
			} catch (Exception ex) {
				logger.error(Constants.STACKTRACE, ex);
			}
		}
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		// need to determine if we will do a 
		// merge vs. a join on the frame
		// we will do this based on 
		String[] origHeaders = this.dataframe.getColumnHeaders();
		List<IQuerySelector> newSelectors = getSelectors();
		int numNew = newSelectors.size();
		String[] newHeaders = new String[numNew];
		for(int i = 0; i < numNew; i++) {
			newHeaders[i] = newSelectors.get(i).getAlias();
		}
		boolean performMerge = allHeadersAccounted(origHeaders, newHeaders, joins);
		if(performMerge) {
			return performMerge(joins, origHeaders, newHeaders);
		} else {
			return performJoin(joins);
		}
	}
	
	private List<IQuerySelector> getSelectors() {
		if(this.qs instanceof HardSelectQueryStruct) {
			// we are querying a frame or engine
			// it is a raw select wrapper
			String[] headers = ((IRawSelectWrapper) this.it).getHeaders();
			List<IQuerySelector> selectors = new Vector<IQuerySelector>();
			for(int i = 0; i < headers.length; i++) {
				selectors.add(new QueryColumnSelector(headers[i]));
			}
			return selectors;
		} else {
			return qs.getSelectors();
		}
	}
	
	private ITableDataFrame performMerge(List<Join> joins, String[] origHeaders, String[] newHeaders) {
		// need to know the starting headers
		// we will lose this once we synchronize the frame with the new header info
		String leftTableName = this.dataframe.getName();
//		testGridData("select * from " + leftTableName);
		Map<String, SemossDataType> leftTableTypes = this.dataframe.getMetaData().getHeaderToTypeMap();
		
		// define a new temporary table with a random name
		// we will flush out the iterator into this table
		String rightTableName = Utility.getRandomString(6);
		Map<String, SemossDataType> rightTableTypes = ImportUtility.getTypesFromQs(this.qs, this.it);
		this.dataframe.addRowsViaIterator(this.it, rightTableName, rightTableTypes);
		
		String mergeTable = rightTableName;
		
		//flag added since createNewTableFromJoiningTables function is called for merge and join, added a default flag here as well
		boolean rightJoinFlag = false;
		
		// if the size is not the same
		// we need to do a join to ensure that what we merge
		// we get the new headers that are required
		if(origHeaders.length != newHeaders.length) {
			// now, i will make another temp table
			// where i do an inner join between the current table and the above new table
			// but we want to remove any of the duplicate headers not present in the join
			String innerJoinTable = Utility.getRandomString(6);
			Set<String> removeHeaders = new HashSet<String>();
			MAIN_LOOP : for(String rightTableHeader : rightTableTypes.keySet()) {
				for(Join join : joins) {
					if(rightTableHeader.equals(join.getRColumn())) {
						// this is a join column
						// do not remove it
						continue MAIN_LOOP;
					}
				}
				for(String leftTableHeader : leftTableTypes.keySet()) {
					String leftTableAlias = leftTableHeader;
					if(leftTableAlias.contains("__")) {
						leftTableAlias = leftTableHeader.split("__")[1];
					}
					if(leftTableAlias.equals(rightTableHeader)) {
						// we found a match that isn't in the join
						// remove it
						removeHeaders.add(leftTableHeader);
					}
				}
			}
			leftTableTypes.keySet().removeAll(removeHeaders);
			String joinQuery = queryUtil.createNewTableFromJoiningTables(innerJoinTable, leftTableName, leftTableTypes, 
					rightTableName, rightTableTypes, joins, new HashMap<String, String>(), new HashMap<String, String>(), rightJoinFlag);
			try {
				this.dataframe.getBuilder().runQuery(joinQuery);
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
//			testGridData("select * from " + innerJoinTable);
			mergeTable = innerJoinTable;
			
			// now drop the right table since we will only be using the innerJoinTable now
			try {
				this.dataframe.getBuilder().runQuery(queryUtil.dropTable(rightTableName));
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		// now we merge the 2 tables together
		// need to get this in the right order
		origHeaders = this.dataframe.getBuilder().getHeaders(leftTableName);
		String[] keyColumns = new String[leftTableTypes.keySet().size()];
		int counter = 0;
		for(String col : leftTableTypes.keySet()) {
			keyColumns[counter] = col.split("__")[1];
			counter++;
		}
		try {
			// the new headers are the keys for the merge
			this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeMergeIntoQuery(leftTableName, mergeTable, keyColumns, origHeaders));
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
//		testGridData("select * from " + leftTableName);
			
		// now drop the merge table
		try {
			this.dataframe.getBuilder().runQuery(this.dataframe.getQueryUtil().dropTable(mergeTable));
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		return this.dataframe;
	}

	private AbstractRdbmsFrame performJoin(List<Join> joins) {
		// need to know the starting headers
		// we will lose this once we synchronize the frame with the new header info
		Map<String, SemossDataType> leftTableTypes = this.dataframe.getMetaData().getHeaderToTypeMap();

		// get the columns and types of the new columns we are about to add
		Map<String, SemossDataType> rightTableTypes = ImportUtility.getTypesFromQs(this.qs, this.it);

		// these will only be used if we have an outer join!
		String leftJoinReturnTableName = null;
		String rightJoinReturnTableName = null;

		// we will also figure out if there are columns that are repeated
		// but are not join columns
		// so we need to alias them
		Set<String> leftTableHeaders = leftTableTypes.keySet();
		Set<String> rightTableHeaders = rightTableTypes.keySet();
		Set<String> rightTableJoinCols = getRightJoinColumns(joins);
		Map<String, String> rightTableAlias = new HashMap<String, String>();
		// note, we are not going to modify the existing headers
		// even though the query builder code allows for it
		Map<String, String> leftTableAlias = new HashMap<String, String>();
		for(String leftTableHeader : leftTableHeaders) {
			if(leftTableHeader.contains("__")) {
				leftTableHeader = leftTableHeader.split("__")[1];
			}
			// instead of making the method return a boolean and then having to perform
			// another ignore case match later on
			// we return the match and do a null check
			String dupRightTableHeader = setIgnoreCaseMatch(leftTableHeader, rightTableHeaders, rightTableJoinCols);
			if(dupRightTableHeader != null) {
				rightTableAlias.put(dupRightTableHeader, leftTableHeader + "_1");
			}
		}

		// we will also create a random table name as the return of this operation
		// dont worry, we will override this back to normal once we are done
		String returnTableName = Utility.getRandomString(6);
		String leftTableName = this.dataframe.getName();
		// we will make the right table from the iterator
		// this will happen in the try catch
		String rightTableName = Utility.getRandomString(6);
		
		boolean successfullyAddedData = true;
		
		//flag added to check for right join , default is false and if its right join this is updated to true.
		// this flag is to make sure the outer join has no issues
		boolean rightJoinFlag = false;
		
		try {
			// now, flush the iterator into the right table 
			this.dataframe.addRowsViaIterator(this.it, rightTableName, rightTableTypes);
	
			// improve performance
			generateIndicesOnJoinColumns(leftTableName, rightTableName, joins);
			
			// merge the two tables together
			if(joins.get(0).getJoinType().equals("outer.join")) {
				// h2 does not support full outer join
				// so we will do a left outer join
				// and then a right outer join
				// and then union them together
				joins.get(0).setJoinType("left.outer.join");
				leftJoinReturnTableName = Utility.getRandomString(6);
				String leftOuterJoin = queryUtil.createNewTableFromJoiningTables(leftJoinReturnTableName, leftTableName, leftTableTypes, rightTableName, 
						rightTableTypes, joins, leftTableAlias, rightTableAlias, rightJoinFlag);
				this.dataframe.getBuilder().runQuery(leftOuterJoin);
				
				joins.get(0).setJoinType("right.outer.join");
				rightJoinReturnTableName = Utility.getRandomString(6);
				String rightOuterJoin = queryUtil.createNewTableFromJoiningTables(rightJoinReturnTableName, leftTableName, leftTableTypes, rightTableName, 
						rightTableTypes, joins, leftTableAlias, rightTableAlias, rightJoinFlag);
				this.dataframe.getBuilder().runQuery(rightOuterJoin);

				// run a union between the 2 tables
				String unionQuery = "CREATE TABLE " + returnTableName + " AS (SELECT * FROM " + leftJoinReturnTableName + " UNION " + " SELECT * FROM " + rightJoinReturnTableName + ")";
				this.dataframe.getBuilder().runQuery(unionQuery);
			} else {
				// this is the normal case
				// we just need to make a basic join query
				rightJoinFlag = true;
				String joinQuery = queryUtil.createNewTableFromJoiningTables(returnTableName, leftTableName, leftTableTypes, rightTableName, 
						rightTableTypes, joins, leftTableAlias, rightTableAlias, rightJoinFlag);
				this.dataframe.getBuilder().runQuery(joinQuery);
			}
//		} catch(EmptyIteratorException e) {
//			// no data was returned from iterator
//			// so the right table wasn't created
//			successfullyAddedData = false;
//			// if we have a non-inner join
//			// add the columns into the frame
//			if(!joins.get(0).getJoinType().equals("inner.join")) {
//				// add columns onto the frame
//				String alterQuery = RdbmsQueryBuilder.alterMissingColumns(leftTableName, rightTableTypes, joins, rightTableAlias, this.dataframe.getQueryUtil());
//				try {
//					this.dataframe.getBuilder().runQuery(alterQuery);
//				} catch (Exception ex) {
//					// if this messes up... not sure what to do now 
//					ex.printStackTrace();
//				}
//			} else {
//				// continue the message up
//				// if we have an inner join and no data
//				// result will be null and we dont want that
//				throw new EmptyIteratorException("Query returned no data. Cannot add new data with existing grid");
//			}
		}
		catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			// now drop the 2 join tables if we used an outer join
			if(leftJoinReturnTableName != null) {
				try {
					this.dataframe.getBuilder().runQuery(queryUtil.dropTable(leftJoinReturnTableName));
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(rightJoinReturnTableName != null) {
				try {
					this.dataframe.getBuilder().runQuery(queryUtil.dropTable(rightJoinReturnTableName));
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			
			// also, drop the temporary right table we created
			if(successfullyAddedData) {
				try {
					this.dataframe.getBuilder().runQuery(queryUtil.dropTable(rightTableName));
				} catch (Exception e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// if we can here without any errors
		// then the sql join was sucessful
		// drop the left table and the right table
		// then rename the return table to be the left table name
		if(successfullyAddedData) {
			try {
				this.dataframe.getBuilder().runQuery(queryUtil.dropTable(leftTableName));
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				this.dataframe.getBuilder().runQuery(queryUtil.alterTableName(returnTableName, leftTableName));
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
		
		// merge the QS so it is accurate
		// but we need to consider if there were headers that have been modified
		for(String rightCol : rightTableAlias.keySet()) {
			List<IQuerySelector> selectors = this.qs.getSelectors();
			int numSelectors = selectors.size();
			for(int i = 0; i < numSelectors; i++) {
				IQuerySelector selector = selectors.get(i);
				String alias =  selector.getAlias();
				if(alias.equals(rightCol)) {
					selector.setAlias(rightTableAlias.get(rightCol));
				}
			}
		}

		updateMetaWithAlias(this.dataframe, this.qs, this.it, joins, rightTableAlias);
		return this.dataframe;
	}
	
	/**
	 * Improve performance by adding indices on the join columns between 2 tables
	 * @param leftTable
	 * @param rightTable
	 * @param joins
	 */
	private void generateIndicesOnJoinColumns(String leftTable, String rightTable, List<Join> joins) {
		for(Join j : joins) {
			String leftCol = j.getLColumn();
			if(leftCol.contains("__")) {
				leftCol = leftCol.split("__")[1];
			}
			String rightCol = j.getRColumn();
			if(rightCol.contains("__")) {
				rightCol = rightCol.split("__")[1];
			}
			generateIndices(leftTable, leftCol);
			generateIndices(rightTable, rightCol);
		}
	}
	
	private void generateIndices(String tableName, String columnName) {
		try {
			this.dataframe.getBuilder().addColumnIndex(tableName, columnName);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
	}
	
//	private void testGridData(String query) {
//		// print out this new table for testing
//		System.out.println(query);
//		System.out.println(query);
//		System.out.println(query);
//		System.out.println(query);
//		ResultSet rs = this.dataframe.execQuery(query);
//		
//		try {
//			ResultSetMetaData rsmd = rs.getMetaData();
//			int numCols = rsmd.getColumnCount();
//			String[] columns = new String[numCols];
//			for(int i = 0; i < numCols; i++) {
//				columns[i] = rsmd.getColumnName(i+1);
//			}
//			System.out.println(Arrays.toString(columns));
//			while(rs.next()) {
//				Object[] data = new Object[numCols];
//				for(int i = 0; i < numCols; i++) {
//					data[i] = rs.getObject(i+1);
//				}
//				System.out.println(Arrays.toString(data));
//			}
//		} catch (SQLException e) {
//			logger.error(Constants.STACKTRACE, e);
//		} finally {
//			try {
//				rs.close();
//			} catch (SQLException e) {
//				logger.error(Constants.STACKTRACE, e);
//			}
//		}
//	}
	
}
