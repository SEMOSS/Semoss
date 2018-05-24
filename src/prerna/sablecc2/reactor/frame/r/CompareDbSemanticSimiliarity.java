package prerna.sablecc2.reactor.frame.r;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.lambda.flatmap.FlatMapLambdaTask;
import prerna.sablecc2.reactor.task.lambda.map.GoogleEntityAnalyzerLambda;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CompareDbSemanticSimiliarity extends AbstractRFrameReactor {
	
	public static final String DATABASES = "databases";
	public static final String UPDATED_BOOL = "updatedBool";

	public CompareDbSemanticSimiliarity(){
		this.keysToGet = new String[]{DATABASES, UPDATED_BOOL};
	}
	
	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		List<String> databaseList = getDatabases();
		
		// check if packages are installed
		String[] packages = { "lsa", "data.table", "WikidataR", "LSAfun", "text2vec","plyr", "stringdist", "XML", "RCurl" };
		this.rJavaTranslator.checkPackages(packages);
		
		//		String frame = this.getFrame().getTableName();
		String rMasterTable1 = "semanticMasterTable1";
		String rTempTable = "semanticTempTable";
		String resultsTable = "semanticResults";
		// source all scripts
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String wrkDir = "setwd(\"" + DIHelper.getInstance().getProperty("BaseFolder") + "\\R\\Recommendations\\SemanticSimilarity\");";
		wrkDir = wrkDir.replace("\\", "/");
		this.rJavaTranslator.runR(wrkDir);
		String sourceScript = "source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\column_doc.r\") ; ";
		sourceScript = sourceScript.replace("\\", "/");
		this.rJavaTranslator.runR(sourceScript);
		String sourceScript2 = "source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\lsi_dataitem.r\") ; ";
		sourceScript2 = sourceScript2.replace("\\", "/");
		this.rJavaTranslator.runR(sourceScript2);
		ArrayList<String> failedColArray = new ArrayList<String>();
		boolean addFlag = getUpdatedBool();
		if(addFlag){
			for (String database : databaseList) {
				IEngine engine = Utility.getEngine(database);
				List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engine.getEngineName());

				// iterate this and if table changes then execute and do R updates
				int count = 0;
				for (Object[] tableCol : allTableCols) {
					SelectQueryStruct qs = new SelectQueryStruct();
					Map typesMap = new HashMap<String, SemossDataType>();
					if (tableCol.length == 4) {
						String table = tableCol[0] + "";
						String col = tableCol[1] + "";
						String dataType = tableCol[2] + "";
						if (dataType.equals(SemossDataType.STRING.toString())) {
							count++;
							QueryColumnSelector colSelector = new QueryColumnSelector();
							colSelector.setTable(table);
							colSelector.setColumn(col);
							colSelector.setAlias(engine.getEngineName() + "$" + table + "$" + col);
							typesMap.put(col, dataType);
							qs.addSelector(colSelector);
							// only values that arnent null
							SimpleQueryFilter nulls = new SimpleQueryFilter(
									new NounMetadata(colSelector, PixelDataType.COLUMN), "!=",
									new NounMetadata("null", PixelDataType.NULL_VALUE));
							qs.addExplicitFilter(nulls);

							// execute query and run r stuff
							IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
							if(!iterator.hasNext()){
								// all values are null in this column
								failedColArray.add(engine.getEngineName() + "$" + table + "$" + col );
								continue;
							}
							// write to csv and read to R
							String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
							File newFile = Utility.writeResultToFile(newFileLoc, iterator, typesMap, "\t");
							String loadFileRScript = rTempTable + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/") + "\", sep=\"\t\");";
							this.rJavaTranslator.runR(loadFileRScript);
							newFile.delete();

							// get random subset
							this.rJavaTranslator.runR(rTempTable + "<-" + rTempTable + "[sample(nrow(" + rTempTable + "),20),c(\"" + engine.getEngineName() + "$" + table + "$" + col + "\")]");
							
							/// update run individually for each column
							this.rJavaTranslator.runR(rMasterTable1 + "<-as.data.frame(" + rMasterTable1 + ");");
							this.rJavaTranslator.runR("errorResults <- column_doc_mgr(" + rMasterTable1 + ",\"column-desc-set\", googleSearch=FALSE);");
							String checkNull = "is.null(errorResults)";
							boolean nullResults = true; 
							nullResults = this.rJavaTranslator.getBoolean(checkNull);
							this.rJavaTranslator.runR("rm(errorResults)");
							if (nullResults){
								// TODO: run Google NLP
								failedColArray.add(engine.getEngineName() + "$" + table + "$" + col );
							}
						}
					} else {
						throw new IllegalArgumentException("The source data isnt good!!!!!");
					}
				}
				if (count == 0){
					throw new IllegalArgumentException("There were no String values to use for exploring!");
				}
				count = 0;
			}

			////////// run georges scripts
			this.rJavaTranslator.runR(rMasterTable1 + "<-as.data.frame(" + rMasterTable1 + ");");
			this.rJavaTranslator.runR("column_lsi_mgr(\"column-desc-set\",\"X4\",\"score\",0.8)");
			this.rJavaTranslator.runR("compute_column_desc_sim(\"column-desc-set\")");
			System.out.println("THESE COLUMNS FAILED!!! :::::::" + failedColArray);
		}
		this.rJavaTranslator.runR(resultsTable + "<-readRDS(\"column-desc-set-sim.rds\");");
		
		// starts with DB
		String test6 = resultsTable + "<-" + resultsTable + "[grep(\"^" + databaseList.get(0) + "\",rownames(" + resultsTable + ")),]";
		this.rJavaTranslator.runR(test6);
		this.rJavaTranslator.runR("library(data.table); " + resultsTable + "<-as.data.table(as.table(" + resultsTable + "));");

		// remove all values less than zero
		this.rJavaTranslator.runR(resultsTable + "[" + resultsTable + "< 0,]= 0");
		
		// update header names
		this.rJavaTranslator.runR(resultsTable + "$" + databaseList.get(0) + "<-" + resultsTable + "$V1");
		this.rJavaTranslator.runR(resultsTable + "$LocalMaster<-" + resultsTable + "$V2");
		this.rJavaTranslator.runR(resultsTable + " <- " + resultsTable + "[,-c(\"V1\",\"V2\")]");
	
		// TABLES
		String tablesResults = "semanticResultsTables";
		this.rJavaTranslator.runR("compute_entity_sim(\"column-desc-set\",\"table-desc-set\", sep=\"$\")");
		String test11 = tablesResults + "<-readRDS(\"table-desc-set-sim.rds\")";
		this.rJavaTranslator.runR(test11);
		this.rJavaTranslator.runR("library(data.table); " + tablesResults + "<-as.data.table(as.table(" + tablesResults + "));");
		String test13 = tablesResults + "[" + tablesResults + "< 0,]= 0";
		this.rJavaTranslator.runR(test13);
		this.rJavaTranslator.runR(tablesResults + "$" + databaseList.get(0) + "<-" + tablesResults + "$V1");
		this.rJavaTranslator.runR(tablesResults + "$LocalMaster<-" + tablesResults + "$V2");
		this.rJavaTranslator.runR(tablesResults + " <- " + tablesResults + "[,-c(\"V1\",\"V2\")]");
		//remove NaN values
		this.rJavaTranslator.runR(tablesResults + "$N[" + tablesResults + "$N==\"NaN\"] <- 0");
		// clean up the decimal places
		this.rJavaTranslator.runR(tablesResults + "$" + databaseList.get(0) + "<-gsub(\"$\",\"\"," + tablesResults+ "$" + databaseList.get(0) + ")");
		
		// create an R datatable
		String dbResults = "semanticResultsDb";
		String databaseScript = "compute_entity_sim(\"column-desc-set\",\"database-desc-set\",\"database\", sep=\"$\")";
		this.rJavaTranslator.runR(databaseScript);
		String test14 = dbResults + "<-readRDS(\"database-desc-set-sim.rds\")";
		this.rJavaTranslator.runR(test14);
		// removes rows that arent from current db we are viewing
		this.rJavaTranslator.runR("library(data.table); " + dbResults + "<-as.data.table(as.table(" + dbResults + "));");
		String test16 = dbResults + "[" + dbResults + "< 0,]= 0";
		this.rJavaTranslator.runR(test16);
		this.rJavaTranslator.runR(dbResults + "$" + databaseList.get(0) + "<-" + dbResults + "$V1");
		this.rJavaTranslator.runR(dbResults + "$LocalMaster<-" + dbResults + "$V2");
		this.rJavaTranslator.runR(dbResults + " <- " + dbResults + "[,-c(\"V1\",\"V2\")]");
		this.rJavaTranslator.runR(dbResults + "$N[" + dbResults + "$N==\"NaN\"] <- 0");
		// make whole number
		this.rJavaTranslator.runR(dbResults + "$N<-" + dbResults +"$N * 100");

		// if this is the only entry then throw a warning
		int rows = this.rJavaTranslator.getInt("nrow(" + dbResults + ");");
		if (rows < 2){
			throw new IllegalArgumentException("Only one entry to compare, run again to view your results!");
		}
		
		// clean garbage
		this.rJavaTranslator.runR("rm(apply_tfidf, build_query_tdm, col2db,column_doc_mgr, compute_column_desc_sim, find_columns_bydesc, "
				+ "get_sim_query, lsi_mgr, build_query_doc, build_tdm, col2tbl, column_lsi_mgr, compute_entity_sim, create_column_doc, "
				+ "find_exist_columns_bydesc, get_similar_doc, match_desc, table_lsi_mgr);");
		
		// return 3 frames
		RDataTable returnTable = createFrameFromVaraible(resultsTable);
		NounMetadata retNoun = new NounMetadata(returnTable, PixelDataType.FRAME);
		this.insight.getVarStore().put(resultsTable, retNoun);

		RDataTable tablesRet = createFrameFromVaraible(tablesResults);
		NounMetadata retNounT = new NounMetadata(tablesRet, PixelDataType.FRAME);
		this.insight.getVarStore().put(tablesResults, retNounT);

		RDataTable dbRet = createFrameFromVaraible(dbResults);
		NounMetadata retNounD = new NounMetadata(dbRet, PixelDataType.FRAME);
		this.insight.getVarStore().put(dbResults, retNounD);

		return new NounMetadata(failedColArray, PixelDataType.VECTOR);
	}
	
	private boolean getUpdatedBool() {
		GenRowStruct boolGrs = this.store.getNoun(UPDATED_BOOL);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}

	private List<String> getDatabases() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(DATABASES);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				List<String> values = columnGrs.getAllStrValues();
				return values;
			}
		}
		// else, we assume it is values in the curRow
		List<String> values = this.curRow.getAllStrValues();
		return values;
	}
}
