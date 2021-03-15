package prerna.sablecc2.reactor.imports;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class RImporter extends AbstractImporter {

	private RDataTable dataframe;
	private SelectQueryStruct qs;
	private Iterator<IHeadersDataRow> it;
	
	public RImporter(RDataTable dataframe, SelectQueryStruct qs) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		try {
			this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
		} catch (Exception e) {
			e.printStackTrace();
			throw new SemossPixelException(
					new NounMetadata("Error occured executing query before loading into frame", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
	}
	
	public RImporter(RDataTable dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
		this.dataframe = dataframe;
		this.qs = qs;
		// generate the iterator
		this.it = it;
		if(this.it == null) {
			try {
				this.it = ImportUtility.generateIterator(this.qs, this.dataframe);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(
						new NounMetadata("Error occured executing query before loading into frame", 
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
	}
	
	@Override
	public void insertData() {
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
		//		RFrameBuilder builder = this.dataframe.getBuilder();
		//		String tableName = this.dataframe.getName();

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

		//		System.out.println(Arrays.toString(builder.getColumnNames()));
		//		System.out.println(Arrays.toString(builder.getColumnTypes()));

		// define new temporary table with random name
		// we will flush out the iterator into a CSV file
		// and use fread() into the temp name
		String tempTableName = Utility.getRandomString(6);
		Map<String, SemossDataType> newColumnsToTypeMap = ImportUtility.getTypesFromQs(this.qs, it);

		try {
			this.dataframe.addRowsViaIterator(this.it, tempTableName, newColumnsToTypeMap);
			// we may need to alias the headers in this new temp table
			for(String oldColName : rightTableAlias.keySet()) {
				this.dataframe.executeRScript(RSyntaxHelper.alterColumnName(tempTableName, oldColName, rightTableAlias.get(oldColName)));
			}
	
			//define parameters that we will pass into mergeSyntax method to get the R command
			String returnTable = this.dataframe.getName();
			String leftTableName = returnTable;
	
			// only a single join type can be passed at a time
			String joinType = null;
			String joinComparator = null;
			Boolean isBetweenJoin = false;
			Set<String> joinTypeSet = new HashSet<>();
			List<Map<String, String>> joinCols = new ArrayList<>();
			for(Join joinItem : joins) {
				joinType = joinItem.getJoinType();
				joinTypeSet.add(joinType);
				joinComparator = joinItem.getComparator();
				//set betweenJoin Flag to true if join in ON between two column conditions
				if (joinComparator!=null && (joinComparator.equals(">=") || joinComparator.equals("<="))) {
					isBetweenJoin = true;
				}
				// in R, the existing column is referenced as frame__column
				// but the R syntax only wants the col
				Map<String, String> joinColMapping = new HashMap<>();
				String jSelector = joinItem.getLColumn();
				if(jSelector.contains("__")) {
					jSelector = jSelector.split("__")[1];
				}
				String jQualifier = joinItem.getRColumn();
				if(jQualifier.contains("__")) {
					jQualifier = jQualifier.split("__")[1];
				}
				joinColMapping.put(jSelector, jQualifier);
				joinCols.add(joinColMapping);
			}
	
			if(joinTypeSet.size() > 1) {
				// perform clean up
				this.dataframe.executeRScript("rm("+tempTableName+")");
				throw new SemossPixelException(
						new NounMetadata("Mixed join conditions cannot be applied on the R frame type", 
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
	
			// need to account for the data types being different for the join columns
			updateTypesForJoin(leftTableTypes, tempTableName, newColumnsToTypeMap, joinCols);
	
			//System.out.println(Arrays.toString(this.dataframe.getColumnNames(tempTableName)));
			//System.out.println(Arrays.toString(this.dataframe.getColumnTypes(tempTableName)));
	
			//generate SQL query when tables are joined on between (>= & <=) condition and execute through sqldf R package
			if(isBetweenJoin) {			
				Map<String, String> leftTableAlias = new HashMap<String, String>();
				//build the SQL query
				AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.POSTGRES);
				String joinQuery = queryUtil.selectFromJoiningTables(leftTableName, leftTableTypes, tempTableName, newColumnsToTypeMap, 
						joins, leftTableAlias, rightTableAlias, false);
	
				try {
					String[] packages = { "sqldf" };
					this.dataframe.getBuilder().getRJavaTranslator().checkPackages(packages);
				} catch(Exception e) {
					throw new IllegalArgumentException("Must install sqldf package in order to perform between joins in R");
				} finally {
	
				}
	
				StringBuilder rsb = new StringBuilder();
				rsb.append("library(\"sqldf\");");
				// probably need to escape inside quotes in the join query?
				joinQuery = joinQuery.replace("\"", "\\\"");
				rsb.append(leftTableName).append(" <- sqldf(\" ").append(joinQuery).append(" \");");
				rsb.append(leftTableName).append(" <- as.data.table(").append(leftTableName).append(");");
				//execute r command
				this.dataframe.executeRScript(rsb.toString());
			} else {
				//execute r command
				String mergeString = RSyntaxHelper.getMergeSyntax(returnTable, leftTableName, tempTableName, joinType, joinCols);
				this.dataframe.executeRScript(mergeString);	
			}
			this.dataframe.removeAllColumnIndex();
			//System.out.println(Arrays.toString(this.dataframe.getColumnNames()));
			//System.out.println(Arrays.toString(this.dataframe.getColumnTypes()));
	
			updateMetaWithAlias(this.dataframe, this.qs, this.it, joins, rightTableAlias);
		} finally {
			// clean r temp table name
			this.dataframe.executeRScript("rm("+tempTableName+")");
		}
		
		return this.dataframe;
	}
	
	/**
	 * R merge syntax for join only works if the types are the same
	 * We will convert the temp table types to match those of the current frame
	 * @param existingTableTypes
	 * @param tempTable
	 * @param newColumnsToTypeMap
	 * @param joins
	 */
	private void updateTypesForJoin(Map<String, SemossDataType> existingTableTypes, 
			String tempTable, 
			Map<String, SemossDataType> newColumnsToTypeMap, 
			List<Map<String, String>> joins) {
		// we will keep the source type as being the intended one
		// and cast the temp table columns into that type
		
		StringBuilder builder = new StringBuilder();
		for(Map<String, String> j : joins) {
			String existingColName = j.keySet().iterator().next();
			SemossDataType existingType = existingTableTypes.get(existingColName);
			if(existingType == null) {
				existingType = existingTableTypes.get(this.dataframe.getName() + "__" + existingColName);
			}
			String joinColumnInTempFrame = j.get(existingColName);
			SemossDataType joinColumnInTempFrameType = newColumnsToTypeMap.get(joinColumnInTempFrame);
			
			// if they are not the same
			// update the new column
			if(existingType != joinColumnInTempFrameType) {
				if(existingType == SemossDataType.STRING) {
					builder.append( RSyntaxHelper.alterColumnTypeToCharacter(tempTable, joinColumnInTempFrame) ).append(";");
					
				} else if(existingType == SemossDataType.INT) {
					builder.append( RSyntaxHelper.alterColumnTypeToInteger(tempTable, joinColumnInTempFrame) ).append(";");

				} else if(existingType == SemossDataType.DOUBLE) {
					builder.append( RSyntaxHelper.alterColumnTypeToNumeric(tempTable, joinColumnInTempFrame) ).append(";");

				} else if(existingType == SemossDataType.FACTOR) {
					builder.append( RSyntaxHelper.alterColumnTypeToFactor(tempTable, joinColumnInTempFrame) ).append(";");

				} else if(existingType == SemossDataType.DATE) {
					builder.append( RSyntaxHelper.alterColumnTypeToDate(tempTable, null, joinColumnInTempFrame) ).append(";");

				} else if(existingType == SemossDataType.TIMESTAMP) {
					builder.append( RSyntaxHelper.alterColumnTypeToDateTime(tempTable, null, joinColumnInTempFrame) ).append(";");

				}
			}
		}
		
		String changeTypeScript = builder.toString();
		if(!changeTypeScript.isEmpty()) {
			this.dataframe.executeRScript(changeTypeScript);
		}
	}
}
