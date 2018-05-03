package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cern.colt.Arrays;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RFrameBuilder;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class RImporter implements IImporter {

	private RDataTable dataframe;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;
	
	public RImporter(RDataTable dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
	}
	
	public RImporter(RDataTable dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = it;
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
		// dataframe has method exposed to insert the information
		this.dataframe.addRowsViaIterator(this.it);
	}

	@Override
	public ITableDataFrame mergeData(List<Join> joins) {
		boolean updateMeta = false;
		RFrameBuilder builder = this.dataframe.getBuilder();
		String tableName = this.dataframe.getTableName();
		
		System.out.println(Arrays.toString(builder.getColumnNames()));
		System.out.println(Arrays.toString(builder.getColumnTypes()));
		
		// define new temporary table with random name
		// we will flush out the iterator into a CSV file
		// and use fread() into the temp name
		String tempTableName = Utility.getRandomString(6);
		Map<String, SemossDataType> newColumnsToTypeMap = ImportUtility.getTypesFromQs(this.qs);
		this.dataframe.addRowsViaIterator(this.it, tempTableName, newColumnsToTypeMap);
	
		if(builder.isEmpty(tempTableName)) {
			if(joins.get(0).getJoinType().equals("inner.join")) {
				// clear the fake table
				builder.evalR("rm(" + tempTableName + ");");
				throw new IllegalArgumentException("Iterator returned no results. Joining this data would result in no data.");
			}
			updateMeta = true;
			// we are merging w/ no data
			// just add an empty column with the column name
			String alterTable = RSyntaxHelper.alterMissingColumns(tableName, newColumnsToTypeMap, joins, new HashMap<String, String>());
			this.dataframe.executeRScript(alterTable);
		} else {
			
			System.out.println(Arrays.toString(this.dataframe.getColumnNames(tempTableName)));
			System.out.println(Arrays.toString(this.dataframe.getColumnTypes(tempTableName)));
			
			//define parameters that we will pass into mergeSyntax method to get the R command
			String returnTable = this.dataframe.getTableName();
			String leftTableName = returnTable;
			String rightTableName = tempTableName;
			
			// only a single join type can be passed at a time
			String joinType = null;
			List<Map<String, String>> joinCols = new ArrayList<Map<String, String>>();
			for(Join joinItem : joins) {
				joinType = joinItem.getJoinType();
				// in R, the existing column is referenced as frame__column
				// but the R syntax only wants the col
				Map<String, String> joinColMapping = new HashMap<String, String>();
				joinColMapping.put(joinItem.getSelector().split("__")[1], joinItem.getQualifier());
				joinCols.add(joinColMapping);
			}
			
			//execute r command
			updateMeta = true;
			String mergeString = RSyntaxHelper.getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols);
			this.dataframe.executeRScript(mergeString);
			this.dataframe.syncHeaders();
			
			System.out.println(Arrays.toString(this.dataframe.getColumnNames()));
			System.out.println(Arrays.toString(this.dataframe.getColumnTypes()));
			
			// clean r temp table name
			this.dataframe.executeRScript("rm("+tempTableName+")");
		}
		
		if(updateMeta) {
			ImportUtility.parseQueryStructToFlatTableWithJoin(this.dataframe, this.qs, tableName, this.it, joins);
		}
		
		return this.dataframe;
	}
}
