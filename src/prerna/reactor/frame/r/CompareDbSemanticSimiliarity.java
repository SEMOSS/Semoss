package prerna.reactor.frame.r;
//package prerna.sablecc2.reactor.frame.r;
//
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.ds.r.RDataTable;
//import prerna.engine.api.IEngine;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.nameserver.utility.MasterDatabaseUtility;
//import prerna.query.querystruct.SelectQueryStruct;
//import prerna.query.querystruct.filters.SimpleQueryFilter;
//import prerna.query.querystruct.selectors.QueryColumnSelector;
//import prerna.rdf.engine.wrappers.WrapperManager;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.execptions.SemossPixelException;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//import prerna.util.Utility;
//import prerna.util.usertracking.AnalyticsTrackerHelper;
//import prerna.util.usertracking.UserTrackerFactory;
//
//
///**
// * This reactor builds an index of the semantic meaning of each column. It
// * generates a descriptions using wiki and google of a sample of the data, then
// * adds it to the local master index. The index is used to compare columns
// * against each other based on semantic meaning. 
// * 
// * There are two options:
// * 
// * 1.) Generate descriptions, add them to the index, then create a frame to make
// * the visualizations. Takes about 3 seconds a column 
// * 2.) If the database was
// * already indexed and hasnt changed we just create the frame for visualizing.
// * 
// */
//public class CompareDbSemanticSimiliarity extends AbstractRFrameReactor {
//	
//	private static final String CLASS_NAME = CompareDbSemanticSimiliarity.class.getName();
//	public static final String UPDATED_BOOL = "updatedBool";
//
//	public CompareDbSemanticSimiliarity(){
//		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), UPDATED_BOOL};
//	}
//	
//	@Override
//	public NounMetadata execute() {
//		init();
//		organizeKeys();
//		Logger logger = getLogger(CLASS_NAME);
//		String database = this.keyValue.get(this.keysToGet[0]);
//		database = database.replace(" ", "_");
//		// check if packages are installed
//		String[] packages = { "lsa", "WikidataR", "text2vec","plyr", "stringdist" };
//		this.rJavaTranslator.checkPackages(packages);
//		
//		//r temp variables
//        String rMasterTable1 = "semanticMasterTable1";
//        String rTempTable = "semanticTempTable";
//        String resultsTable = "semResults";
//        String seperator = "$";
//
//        // frame name
//        String finalResultFrame = "semanticResults";
//        // frame column names
//        String lmColumns = "LocalMasterColumns";
//        String lmTables = "LocalMasterTables";
//        String lmDatabases = "LocalMasterDatabases";
//        
//        StringBuilder rsb = new StringBuilder();
//        // clean previous tables if re-running
//        rsb.append("rm(" + rMasterTable1 + ", " + rTempTable + "," + resultsTable + ", " + finalResultFrame + ");\n");
//
//        // source all scripts
//        String wd = "wd"+ Utility.getRandomString(5);
//		rsb.append(wd + "<- getwd();");
//        String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
//        
//        rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\");\n");
//        rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\column_doc.r\");\n");
//        rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\lsi_data.r\");\n");
//        this.rJavaTranslator.runR(rsb.toString().replace("\\", "/"));
//        ArrayList<String> failedColArray = new ArrayList<String>();
//        // indexing: skip if user already indexed this db and doesnt want to update the data
//        boolean addFlag = getUpdatedBool();
//		if(addFlag){
//				String dbAlias = MasterDatabaseUtility.testEngineIdIfAlias(database);
//				IEngine engine = Utility.getEngine(dbAlias);
//				List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engine.getEngineId());
//
//				// iterate all columns in db and run R script to generate 
//				// description and add to local master index
//				int count = 0;
//				for (Object[] tableCol : allTableCols) {
//					SelectQueryStruct qs = new SelectQueryStruct();
//					Map typesMap = new HashMap<String, SemossDataType>();
//					if (tableCol.length == 4) {
//						String table = tableCol[0] + "";
//						String col = tableCol[1] + "";
//						String dataType = tableCol[2] + "";
//						if (dataType.equals(SemossDataType.STRING.toString())) {
//							count++;
//							// we will fill this in once we figure out if it is a concept or property
//							QueryColumnSelector colSelector = null;
//							// this is a hack we used in advanced federate
//							if(engine.getParentOfProperty(col + "/" + table) == null) {
//								// we couldn't find a parent for this property
//								// this means it is a concept itself
//								// and we should only use table
//								colSelector = new QueryColumnSelector(table);
//							} else {
//								colSelector = new QueryColumnSelector(table + "__" + col);
//							}
//							qs.addSelector(colSelector);
//							
//							// select only non-null values from database
//							SimpleQueryFilter nulls = new SimpleQueryFilter(
//									new NounMetadata(colSelector, PixelDataType.COLUMN), "!=",
//									new NounMetadata("null", PixelDataType.NULL_VALUE));
//							qs.addExplicitFilter(nulls);
//							IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
//							if(!iterator.hasNext()){
//								// all values are null in this column
//								failedColArray.add(database + seperator + table + seperator + col );
//								continue;
//							}
//							StringBuilder sb = new StringBuilder();
//
//							// write to csv and read into R
//							String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
//							File newFile = writeResultToFile(newFileLoc, iterator, engine.getEngineName() + seperator + table + seperator + col);
//							String loadFileRScript = rTempTable + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/") + "\", sep=\"\t\");\n";
//							sb.append(loadFileRScript);
//
//							// get random subset of column data
//							sb.append("if(nrow(" + rTempTable + ") > 20) {");
//							sb.append(rTempTable + "<-" + rTempTable + "[sample(nrow(" + rTempTable + "),20),c(");
//							sb.append("\"" + database + seperator + table + seperator + col + "\"");
//							sb.append(")];}\n");
//
//							// execute script to get descriptions for this column
//							sb.append(rTempTable + "<-as.data.frame(" + rTempTable + ");\n");
//							logger.info("Adding " + col + " from table " + table + " and database " + database + " to the local master index...");
//							sb.append("errorResults <- column_doc_mgr(" + rTempTable + ",\"column-desc-set\");\n");
//							this.rJavaTranslator.runR(sb.toString());
//							
//							newFile.delete();
//							String checkNull = "is.null(errorResults)";
//							boolean nullResults = true; 
//							nullResults = this.rJavaTranslator.getBoolean(checkNull);
//							this.rJavaTranslator.runR("rm(errorResults)");
//							
//							// keep track of any columns that fail
//							if (nullResults){
//								failedColArray.add(database + seperator + table + seperator + col );							
//							}
//						}
//					} 
//				}
//			if (count == 0) {
//				this.rJavaTranslator.runR("setwd(" + wd + ");\n");
//				this.rJavaTranslator.runR("rm(apply_tfidf, build_query_tdm, col2db,column_doc_mgr, "
//						+ "compute_column_desc_sim, find_columns_bydesc, "
//						+ "get_sim_query, lsi_mgr, build_query_doc, build_tdm, col2tbl, "
//						+ "column_lsi_mgr, compute_entity_sim, create_column_doc, "
//						+ "find_exist_columns_bydesc, get_similar_doc, match_desc, "
//						+ "table_lsi_mgr,construct_column_doc, constructName,  discover_column_desc, "
//						+ "get_column_desc,get_column_desc_alt, getSearchURL);");
//				NounMetadata noun = new NounMetadata("There were no String values to use for exploring!", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
//				SemossPixelException exception = new SemossPixelException(noun);
//				exception.setContinueThreadOfExecution(false);
//				throw exception;
//			}
//
//			// run lsi scripts to build index
//			this.rJavaTranslator.runR(rTempTable + "<-as.data.frame(" + rTempTable + ");");
//			this.rJavaTranslator.runR("column_lsi_mgr(\"column-desc-set\",\"X4\",\"score\",0.8)");
//			this.rJavaTranslator.runR("compute_column_desc_sim(\"column-desc-set\")");
//			logger.info("THESE COLUMNS FAILED!!! " + failedColArray);
//		}
//
//		// The scripts creates 3 separate files one for columns vs LM, tables vs LM,
//		// and database vs LM. So we have to read them and create one final frame to
//		// visualize off of.
//		
//		// read index files and generate frame for the visualizations.
//		String rSemanticResultsMatrix = "semMatrix";
//
//        // create results frame r code
//        rsb = new StringBuilder();
//        rsb.append(rSemanticResultsMatrix + "<-readRDS(\"column-desc-set-sim.rds\");\n");
//        
//        // only keep columns from database we are looking at
//        rsb.append(rSemanticResultsMatrix + "<-" + rSemanticResultsMatrix + "[grep(\"^" + database + "\",rownames(" + rSemanticResultsMatrix + ")),];\n");
//        rsb.append("library(data.table);\n" + resultsTable + "<-as.data.table(as.table(" + rSemanticResultsMatrix + "));\n");
//        
//        // check if resultsTable exists
//        rsb.append("if(exists('" + resultsTable + "')) {\n");
//
//        // set all values less than zero to zero
//        rsb.append(resultsTable + "[" + resultsTable + "< 0,]= 0;\n");
//        
//        // update header names
//		rsb.append("colnames(" + resultsTable + ")[1] <- \"" + database + "\";\n");
//		rsb.append("colnames(" + resultsTable + ")[2] <-\"" + lmColumns + "\";\n");
//        
//        // compute and read in similar tables set
//        String tablesResults = "semanticResultsTables";
//        rsb.append("\n\n");
//        rsb.append("compute_entity_sim(\"column-desc-set\",\"table-desc-set\", sep=\"" + seperator + "\");\n");
//        String rTableRDS = "tableRDS";
//        rsb.append(rTableRDS + "<-readRDS(\"table-desc-set-sim.rds\");\n");
//        rsb.append(tablesResults + "<-as.data.table(as.table(" + rTableRDS + "));\n");
//        
//        // check if tablesResults exists
//        rsb.append("if(exists('" + tablesResults + "')) {\n");
//
//        // rename column names
//        rsb.append("colnames(" + tablesResults + ")[1] <- \"" + database + "\";\n");
//        rsb.append("colnames(" + tablesResults + ")[2] <- \"" + lmTables + "\";\n");
//        rsb.append(tablesResults + "[" + tablesResults + "< 0,]= 0;\n");
//        
//        // remove NaN values
//        rsb.append(tablesResults + "$N[" + tablesResults + "$N==\"NaN\"] <- 0;\n");
//        
//        // clean up dollar sign from end of database name
//        rsb.append(tablesResults + "$" + database + "<-gsub(\"" + seperator + "\",\"\"," + tablesResults + "$" + database + ");\n");
//        rsb.append("\n\n");
//        
//        // compute and read in similar databases set
//        String dbResults = "semanticResultsDb";
//        String rDbRDS = "dbRDS";
//        String databaseScript = "compute_entity_sim(\"column-desc-set\",\"database-desc-set\",\"database\", sep=\"" + seperator + "\");\n";
//        rsb.append(databaseScript);
//        rsb.append(rDbRDS + "<- readRDS(\"database-desc-set-sim.rds\");\n");
//        
//        // removes rows that arent from current db we are viewing
//        rsb.append(dbResults + "<-as.data.table(as.table(" + rDbRDS + "));\n");
//        rsb.append("if(exists('" + dbResults + "')) {\n");
//        
//        // filter table using db
//        rsb.append(dbResults+" <-" + dbResults + "[which(" + dbResults + "$V1 == '" + database + "')];\n");
//        
//        // change column headers for dbResults
//        rsb.append("colnames(" + dbResults + ")[1] <- \"" + database + "\";\n");
//        rsb.append("colnames(" + dbResults + ")[2] <- \"" + lmDatabases +"\";\n");
//        
//        // clean data for dbResults
//        rsb.append(dbResults + "[" + dbResults + "< 0,]= 0;\n");
//        rsb.append(dbResults + "$N[" + dbResults + "$N==\"NaN\"] <- 0;\n");
//        rsb.append(resultsTable + "<-cSplit(" + resultsTable + ", \"" + lmColumns + "\", sep=\"" + seperator + "\", direction=\"wide\", drop = FALSE);\n");
//        rsb.append(resultsTable + "<-cSplit(" + resultsTable + ", \"" + database + "\", sep=\"" + seperator + "\", direction=\"wide\", drop = FALSE);\n");
//        rsb.append("colnames("+resultsTable+")[4] <- \"" + lmDatabases + "\";\n");
//        
//        // create unique ids so we can merge together the 3 result 
//        // frames (column data, table data, and database data)
//        rsb.append(resultsTable + "$tableid1 <- apply( " + resultsTable + "[ , c('" + lmDatabases + "','" + lmColumns + "_2') ] , 1 , paste , collapse = \"" + seperator + "\" );\n");
//        rsb.append(resultsTable + "$tableid2 <- apply( " + resultsTable + "[ , c('" + database + "_1','" + database + "_2') ] , 1 , paste , collapse = \"" + seperator + "\" );\n");
//        rsb.append(resultsTable + "$tableid <- apply( " + resultsTable + "[ , c('tableid1','tableid2') ] , 1 , paste , collapse = \"===\" );\n");
//        rsb.append("colnames("+resultsTable+")[3] <- \""+"NCol"+"\";\n");
//        rsb.append(resultsTable + " <- " + resultsTable + "[,c('NCol','" + database + "','" + lmColumns + "', 'tableid', '" + lmDatabases + "')];\n");
//
//        rsb.append("colnames(" + tablesResults + ")[3] <- \"" + "NTable" + "\";\n");
//        rsb.append("colnames(" + tablesResults + ")[1] <- \"" + database + "Table" + "\";\n");
//        rsb.append(tablesResults + "$tableid <- apply( " + tablesResults + "[ , c('" + lmTables + "','" + database + "Table') ] , 1 , paste , collapse = \"===\" );\n");
//
//        // rename db table colnames
//        rsb.append("colnames(" + dbResults + ")[3] <- \"" + "NDb" + "\";\n");
//        rsb.append("colnames(" + dbResults + ")[1] <- \"" + database + "Db" + "\";\n");
//        
//        // merge all three frames togther using the unique ids
//        rsb.append(resultsTable + "<- merge(" + resultsTable + "," + tablesResults + ", by='tableid');\n");
//        rsb.append(resultsTable + "<- merge(" + resultsTable + "," + dbResults + ", by='" + lmDatabases + "');\n");
//
//        // keep only the columns we need for the visualizations
//        rsb.append(finalResultFrame + " <- " + resultsTable + "[,c('NCol','NDb','NTable','" + database + "','" + database + "Table','" + database + "Db','" + lmColumns + "', '" + lmTables + "', '" + lmDatabases + "')];\n");
//        rsb.append("}\n");
//        rsb.append("}\n");
//        rsb.append("}\n");
//        rsb.append("setwd("+wd+");\n");
//        // clean garbage
//        rsb.append("rm(apply_tfidf, build_query_tdm, col2db,column_doc_mgr, "
//                    + "compute_column_desc_sim, find_columns_bydesc, "
//                    + "get_sim_query, lsi_mgr, build_query_doc, build_tdm, col2tbl, "
//                    + "column_lsi_mgr, compute_entity_sim, create_column_doc, "
//                    + "find_exist_columns_bydesc, get_similar_doc, match_desc, "
//                    + "table_lsi_mgr,construct_column_doc, constructName,  discover_column_desc, "
//                    + "get_column_desc,get_column_desc_alt, getSearchURL, " + rDbRDS + ","
//                    + tablesResults + ", " + rSemanticResultsMatrix + ", " + rTableRDS + ", " + resultsTable + ", " + dbResults + ");");
//        this.rJavaTranslator.runR(rsb.toString());
//
//        // check if process failed or if its the first entry in the 
//        // index, in which case we cant visualize it yet
//        String frameExists = "exists('" + finalResultFrame + "')";
//        boolean nullResults = this.rJavaTranslator.getBoolean(frameExists);
//        if (!nullResults) {
//			NounMetadata noun = new NounMetadata("Unable to view your results. Must have more than one database indexed.", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
//        	SemossPixelException exception = new SemossPixelException(noun);
//			exception.setContinueThreadOfExecution(false);
//			throw exception;
//        }
//
//        RDataTable returnTable = createNewFrameFromVariable(finalResultFrame);
//        this.insight.setDataMaker(returnTable);
//        
//		// NEW TRACKING
//		UserTrackerFactory.getInstance().trackAnalyticsWidget(
//				this.insight, 
//				null, 
//				"CompareDbSemanticSimilarity", 
//				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
//        
//        return new NounMetadata(returnTable, PixelDataType.FRAME);
//	}
//	
//	private boolean getUpdatedBool() {
//		GenRowStruct boolGrs = this.store.getNoun(UPDATED_BOOL);
//		if(boolGrs != null) {
//			if(boolGrs.size() > 0) {
//				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
//				return (boolean) val.get(0);
//			}
//		}
//		return true;
//	}
//	
//	public static File writeResultToFile(String fileLocation, Iterator<IHeadersDataRow> it, String header) {
//		long start = System.currentTimeMillis();
//
//		// make sure file is empty so we are only inserting the new values
//		File f = new File(fileLocation);
//		if (f.exists()) {
//			System.out.println("File currently exists.. deleting file");
//			f.delete();
//		}
//		try {
//			f.createNewFile();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		FileWriter writer = null;
//		BufferedWriter bufferedWriter = null;
//
//		try {
//			writer = new FileWriter(f);
//			bufferedWriter = new BufferedWriter(writer);
//
//			StringBuilder builder = new StringBuilder();
//			// add header
//			builder.append("\"").append(header).append("\"\n");
//			// now loop through all the data
//			while (it.hasNext()) {
//				IHeadersDataRow row = it.next();
//				// generate the data row
//				Object[] dataRow = row.getValues();
//				// assumes all are strings
//				builder.append("\"").append(dataRow[0]).append("\"\n");
//			}
//			bufferedWriter.write(builder.toString());
//
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (bufferedWriter != null) {
//					bufferedWriter.close();
//				}
//				if (writer != null) {
//					writer.close();
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//
//		long end = System.currentTimeMillis();
//		System.out.println("Time to output file = " + (end - start) + " ms");
//
//		return f;
//	}
//}
