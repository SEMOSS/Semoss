package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import cern.colt.Arrays;
import prerna.algorithm.api.IMetaData.DATA_TYPES;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.Join;
import prerna.util.Utility;

public class RImporter implements IImporter {

	private RDataTable dataframe;
	private QueryStruct2 qs;
	private Iterator<IHeadersDataRow> it;
	
	public RImporter(RDataTable dataframe, QueryStruct2 qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
	}
	
	public RImporter(RDataTable dataframe, QueryStruct2 qs, Iterator<IHeadersDataRow> it) {
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
		System.out.println(Arrays.toString(this.dataframe.getColumnNames()));
		System.out.println(Arrays.toString(this.dataframe.getColumnTypes()));
		
		ImportUtility.parseQueryStructToFlatTableWithJoin(this.dataframe, this.qs, this.dataframe.getTableName(), this.it, joins);
		
		// define new temporary table with random name
		// we will flush out the iterator into a CSV file
		// and use fread() into the temp name
		String tempTableName = Utility.getRandomString(6);
		Map<String, DATA_TYPES> types = ImportUtility.getTypesFromQs(this.qs);
		this.dataframe.addRowsViaIterator(this.it, tempTableName, types);
	
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
		String mergeString = RSyntaxHelper.getMergeSyntax(returnTable, leftTableName, rightTableName, joinType, joinCols);
		this.dataframe.executeRScript(mergeString);
		this.dataframe.syncHeaders();
		
		System.out.println(Arrays.toString(this.dataframe.getColumnNames()));
		System.out.println(Arrays.toString(this.dataframe.getColumnTypes()));

		return this.dataframe;
	}
}
