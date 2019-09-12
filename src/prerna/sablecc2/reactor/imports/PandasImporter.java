package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class PandasImporter extends AbstractImporter {

	private PandasFrame dataframe;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;
	
	public PandasImporter(PandasFrame dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		try {
			this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(
					new NounMetadata("Error occured executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
	}
	
	public PandasImporter(PandasFrame dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = it;
	}
	
	@Override
	public void insertData() {
		// pre making the frame name
		ImportUtility.parseQueryStructToFlatTable(this.dataframe, this.qs, this.dataframe.getName(), this.it);
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
		// need to ensure no names overlap
		// otherwise we R will create a weird renaming for the column
		Map<String, SemossDataType> leftTableTypes = this.dataframe.getMetaData().getHeaderToTypeMap();

		// get the columns and types of the new columns we are about to add
		Map<String, SemossDataType> rightTableTypes = ImportUtility.getTypesFromQs(this.qs, this.it);

		// we will also figure out if there are columns that are repeated
		// but are not join columns
		// so we need to alias them
		Set<String> leftTableHeaders = leftTableTypes.keySet();
		Set<String> rightTableHeaders = rightTableTypes.keySet();
		Set<String> rightTableJoinCols = getRightJoinColumns(joins);
		
		Map<String, String> rightTableAlias = new HashMap<String, String>();

		// note, we are not going to modify the existing headers
		// even though the query builder code allows for it
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
		
		String tempTableName = Utility.getRandomString(6);
		Map<String, SemossDataType> newColumnsToTypeMap = ImportUtility.getTypesFromQs(this.qs, it);
		this.dataframe.addRowsViaIterator(this.it, tempTableName, newColumnsToTypeMap);
		
		// we may need to alias the headers in this new temp table
		if(!rightTableAlias.isEmpty()) {
			for(String oldColName : rightTableAlias.keySet()) {
				this.dataframe.runScript(PandasSyntaxHelper.alterColumnName(tempTableName, oldColName, rightTableAlias.get(oldColName)));
			}
		}
		
		if(this.dataframe.isEmpty(tempTableName)) {
//			if(joins.get(0).getJoinType().equals("inner.join")) {
				// clear the fake table
				this.dataframe.runScript("del " + tempTableName);
				throw new IllegalArgumentException("Iterator returned no results. Joining this data would result in no data.");
//			}
			// TODO: figure out this annoying "not in index" error that i get here
//			// we are merging w/ no data
//			// just add an empty column with the column name
//			String alterTable = PandasSyntaxHelper.alterMissingColumns(this.dataframe.getTableName(), this.dataframe.getColumnHeaders(), newColumnsToTypeMap, joins, new HashMap<String, String>());
//			this.dataframe.runScript(alterTable);
		} else {
		
			//define parameters that we will pass into mergeSyntax method to get the R command
			String returnTable = this.dataframe.getName();
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
				String jSelector = joinItem.getSelector();
				if(jSelector.contains("__")) {
					jSelector = jSelector.split("__")[1];
				}
				String jQualifier = joinItem.getQualifier();
				if(jQualifier.contains("__")) {
					jQualifier = jQualifier.split("__")[1];
				}
				joinColMapping.put(jSelector, jQualifier);
				joinCols.add(joinColMapping);
			}
			
			//execute r command
			this.dataframe.merge(returnTable, leftTableName, rightTableName, joinType, joinCols);
		}
		
		updateMetaWithAlias(this.dataframe, this.qs, this.it, joins, rightTableAlias);
		return this.dataframe;
	}
}
