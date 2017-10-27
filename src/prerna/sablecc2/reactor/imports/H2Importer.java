package prerna.sablecc2.reactor.imports;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.h2.H2Frame;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.Join;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class H2Importer implements IImporter {

	private H2Frame dataframe;
	private QueryStruct2 qs;
	private Iterator<IHeadersDataRow> it;
	
	public H2Importer(H2Frame dataframe, QueryStruct2 qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		this.it = it;
	}
	
	public H2Importer(H2Frame dataframe, QueryStruct2 qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
	}

	@Override
	public void insertData() {
		ImportUtility.parseQueryStructToFlatTable(this.dataframe, this.qs, this.dataframe.getTableName(), this.it);
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
		Map<String, IMetaData.DATA_TYPES> rawDataTypeMap = this.dataframe.getMetaData().getHeaderToTypeMap();
		
		// TODO: this is annoying, need to get the frame on the same page as the meta
		Map<String, IMetaData.DATA_TYPES> dataTypeMap = new HashMap<String, IMetaData.DATA_TYPES>();
		for(String rawHeader : rawDataTypeMap.keySet()) {
			dataTypeMap.put(rawHeader.split("__")[1], rawDataTypeMap.get(rawHeader));
		}
		
		// use the base table name
		String tableName = this.dataframe.getTableName();
		try {
			this.dataframe.addRowsViaIterator(this.it, tableName, dataTypeMap);
		} catch(Exception e) {
			// if we have an error
			// just make sure the headers are all there
			int size = dataTypeMap.size();
			String[] newHeaders = new String[size];
			String[] newTypes = new String[size];
			int counter = 0;
			for(String header : dataTypeMap.keySet()) {
				newHeaders[counter] = header;
				newTypes[counter] = Utility.convertDataTypeToString(dataTypeMap.get(header));
				counter++;
			}
			try {
				this.dataframe.getBuilder().alterTableNewColumns(tableName, newHeaders, newTypes);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		// need to determine if we will do a 
		// merge vs. a join on the frame
		// we will do this based on 
		String[] origHeaders = this.dataframe.getColumnHeaders();
		List<IQuerySelector> newSelectors = this.qs.getSelectors();
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
	
	private ITableDataFrame performMerge(List<Join> joins, String[] origHeaders, String[] newHeaders) {
		// need to know the starting headers
		// we will lose this once we synchronize the frame with the new header info
		String leftTableName = this.dataframe.getTableName();
//		testGridData("select * from " + leftTableName);
		Map<String, IMetaData.DATA_TYPES> leftTableTypes = this.dataframe.getMetaData().getHeaderToTypeMap();
		
		// define a new temporary table with a random name
		// we will flush out the iterator into this table
		String rightTableName = Utility.getRandomString(6);
		Map<String, IMetaData.DATA_TYPES> rightTableTypes = ImportUtility.getTypesFromQs(this.qs);
		this.dataframe.addRowsViaIterator(this.it, rightTableName, rightTableTypes);
		
		String mergeTable = rightTableName;
		
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
					if(rightTableHeader.equals(join.getQualifier())) {
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
			String joinQuery = RdbmsQueryBuilder.createNewTableFromJoiningTables(innerJoinTable, leftTableName, leftTableTypes, rightTableName, rightTableTypes, joins);
			try {
				this.dataframe.getBuilder().runQuery(joinQuery);
			} catch (Exception e) {
				e.printStackTrace();
			}
//			testGridData("select * from " + innerJoinTable);
			mergeTable = innerJoinTable;
			
			// now drop the right table since we will only be using the innerJoinTable now
			try {
				this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(rightTableName));
			} catch (Exception e) {
				e.printStackTrace();
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
			e.printStackTrace();
		}
//		testGridData("select * from " + leftTableName);
			
		// now drop the merge table
		try {
			this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(mergeTable));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return this.dataframe;
	}

	private H2Frame performJoin(List<Join> joins) {
		// need to know the starting headers
		// we will lose this once we synchronize the frame with the new header info
		Map<String, IMetaData.DATA_TYPES> leftTableTypes = this.dataframe.getMetaData().getHeaderToTypeMap();
		
		// define a new temporary table with a random name
		// we will flush out the iterator into this table
		String tempTableName = Utility.getRandomString(6);
		Map<String, IMetaData.DATA_TYPES> rightTableTypes = ImportUtility.getTypesFromQs(this.qs);
		this.dataframe.addRowsViaIterator(this.it, tempTableName, rightTableTypes);

		// we will also create a random table name as the return of this operation
		// dont worry, we will override this back to normal once we are done
		String returnTableName = Utility.getRandomString(6);
		String leftTableName = this.dataframe.getTableName();
		String rightTableName = tempTableName;
		
		// these will only be used if we have an outer join!
		String leftJoinReturnTableName = null;
		String rightJoinReturnTableName = null;

		try {
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
				String leftOuterJoin = RdbmsQueryBuilder.createNewTableFromJoiningTables(leftJoinReturnTableName, leftTableName, leftTableTypes, rightTableName, rightTableTypes, joins);
				this.dataframe.getBuilder().runQuery(leftOuterJoin);
				
				joins.get(0).setJoinType("right.outer.join");
				rightJoinReturnTableName = Utility.getRandomString(6);
				String rightOuterJoin = RdbmsQueryBuilder.createNewTableFromJoiningTables(rightJoinReturnTableName, leftTableName, leftTableTypes, rightTableName, rightTableTypes, joins);
				this.dataframe.getBuilder().runQuery(rightOuterJoin);

				// run a union between the 2 tables
				String unionQuery = "CREATE TABLE " + returnTableName + " AS (SELECT * FROM " + leftJoinReturnTableName + " UNION " + " SELECT * FROM " + rightJoinReturnTableName + ")";
				this.dataframe.getBuilder().runQuery(unionQuery);
			} else {
				// this is the normal case
				// we just need to make a basic join query
				String joinQuery = RdbmsQueryBuilder.createNewTableFromJoiningTables(returnTableName, leftTableName, leftTableTypes, rightTableName, rightTableTypes, joins);
				this.dataframe.getBuilder().runQuery(joinQuery);
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			// now drop the 2 join tables if we used an outer join
			if(leftJoinReturnTableName != null) {
				try {
					this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(leftJoinReturnTableName));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if(rightJoinReturnTableName != null) {
				try {
					this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(rightJoinReturnTableName));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			// also, drop the temporary right table we created
			try {
				this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(rightTableName));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// if we can here without any errors
		// then the sql join was sucessful
		// drop the left table and the right table
		// then rename the return table to be the left table name
		try {
			this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeDropTable(leftTableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		try {
			this.dataframe.getBuilder().runQuery(RdbmsQueryBuilder.makeRenameTable(returnTableName, leftTableName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// merge the QS so it is accurate
		ImportUtility.parseQueryStructToFlatTableWithJoin(this.dataframe, this.qs, this.dataframe.getTableName(), this.it, joins);
		this.dataframe.syncHeaders();
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
			String leftCol = j.getSelector();
			if(leftCol.contains("__")) {
				leftCol = leftCol.split("__")[1];
			}
			String rightCol = j.getQualifier();
			if(rightCol.contains("__")) {
				rightCol = rightCol.split("__")[1];
			}
			generateIndices(leftTable, leftCol);
			generateIndices(rightTable, rightCol);
		}
	}
	
	private void generateIndices(String tableName, String columnName) {
		try {
			this.dataframe.getBuilder().runQuery("CREATE INDEX ON " + tableName + " (" + columnName + ")");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Determine if all the headers are taken into consideration within the iterator
	 * This helps to determine if we need to perform an insert vs. an update query to fill the frame
	 * @param headers1				The original set of headers in the frame
	 * @param headers2				The new set of headers from the iterator
	 * @param joins					Needs to take into consideration the joins since we can join on 
	 * 								columns that do not have the same names
	 * @return
	 */
	protected boolean allHeadersAccounted(String[] startHeaders, String[] newHeaders, List<Join> joins) {
		int newHeadersSize = newHeaders.length;
		for(int i = 0; i <  newHeadersSize; i++) {
			// need each of the new headers to be included in the start headers
			if(!ArrayUtilityMethods.arrayContainsValue(startHeaders, newHeaders[i])) {
				// need to account for join 
				for(Join join : joins) {
					String startNode = join.getSelector();
					if(startNode.contains("__")) {
						startNode = startNode.split("__")[1];
					}
					String endNode = join.getQualifier();
					if(newHeaders[i].equals(endNode)) {
						continue;
					} else {
						return false;
					}
				}
			}
		}
		// we were able to iterate through all the new headers
		// and each one exists in the starting headers
		// so all of them are taking into consideration
		return true;
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
//			e.printStackTrace();
//		} finally {
//			try {
//				rs.close();
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
//		}
//	}
	
}
