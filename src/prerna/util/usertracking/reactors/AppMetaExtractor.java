package prerna.util.usertracking.reactors;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.frame.r.CompareDbSemanticSimiliarity;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.usertracking.TrackRequestThread;
import prerna.util.usertracking.UserTrackerFactory;

/**
 * Generates column descriptions and stores in the tracking database Adds unique
 * count to owl file for each column
 *
 */
public class AppMetaExtractor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = AppMetaExtractor.class.getName();
	public static final String DESCRIPTIONS_BOOL = "descriptions";

	public AppMetaExtractor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), DESCRIPTIONS_BOOL };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get inputs - engine
		String engineId = UploadInputUtility.getAppName(this.store);
		// we may have the alias
		if(AbstractSecurityUtils.securityEnabled()) {
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(!SecurityQueryUtils.getUserEngineIds(this.insight.getUser()).contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist or user does not have access to database");
			}
		} else {
			engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
			if(!MasterDatabaseUtility.getAllEngineIds().contains(engineId)) {
				throw new IllegalArgumentException("Database " + engineId + " does not exist");
			}
		}
		
		boolean descriptions = getDescriptionsBool();
		IEngine engine = Utility.getEngine(engineId);

		// validate engine exists
		if (engine == null) {
			throw new IllegalArgumentException("Engine does not exist");
		}
		String owlPath = engine.getOWL();

		// only executes for rdbms, tinker, and rdf
		ENGINE_TYPE engineType = engine.getEngineType();
		if (engineType.equals(ENGINE_TYPE.RDBMS) || engineType.equals(ENGINE_TYPE.SESAME)
				|| engineType.equals(ENGINE_TYPE.TINKER)) {
			OWLER owl = new OWLER(engine, owlPath);
			owl.addUniqueCounts(engine);
		}

		if (UserTrackerFactory.isTracking()) {
			// store descriptions if requested
//			if (descriptions) {
				storeColumnDescriptions(engine);
//			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	private boolean getDescriptionsBool() {
		GenRowStruct boolGrs = this.store.getNoun(DESCRIPTIONS_BOOL);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}

	private void storeColumnDescriptions(IEngine engine) {
		String[] packages = new String[] { "data.table", "WikidataR", "curl", "doParallel", "XML" };
		Logger logger = this.getLogger(CLASS_NAME);
		this.rJavaTranslator.checkPackages(packages);
		int stepCounter = 1;
		logger.info(stepCounter + ". Loading R scripts to store column descriptions");
		StringBuilder rsb = new StringBuilder();
		String wd = "wd" + Utility.getRandomString(5);
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		rsb.append(wd + "<- getwd();");
		rsb.append("setwd(\"" + baseFolder + "\\R\\Recommendations\");\n");
		rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\lsi_dataitem.r\");\n");
		rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\db_recom.r\");\n");
		rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\datasemantic.r\");\n");
		rsb.append("source(\"" + baseFolder + "\\R\\Recommendations\\topic_modelling.r\");\n");
		this.rJavaTranslator.runR(rsb.toString().replace("\\", "/"));
		logger.info(stepCounter + ". Done");
		stepCounter++;

		// GENERATING DESCRIPTIONS
		logger.info(stepCounter + ". Getting Database schema to generate descriptions");
		String rTempTable = "semanticTempTable";
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engine.getEngineId());
		String engineName = engine.getEngineName();
		String engineID = engine.getEngineId();
		String seperator = "$";
		List<Object[]> list = new ArrayList<Object[]>();
		logger.info(stepCounter + ". Done");
		stepCounter++;


		// iterate through all the rows and sample about 15 rows from each
		// of
		// those
		logger.info(stepCounter + ". Processing columns to find descriptions");
		for (Object[] tableCol : allTableCols) {
			SelectQueryStruct qs = new SelectQueryStruct();
			if (tableCol.length == 4) {
				String table = tableCol[0] + "";
				String col = tableCol[1] + "";
				String dataType = tableCol[2] + "";
				boolean primFlag = false;
				String descriptions = "";

				// Only generate column descriptions if the data type is a
				// string
				if (dataType.equals(SemossDataType.STRING.toString())) {
					// we will fill this in once we figure out if it is a
					// concept or property
					QueryColumnSelector colSelector = null;
					// this is a hack we used in advanced federate
					if (engine.getParentOfProperty(col + "/" + table) == null) {
						// we couldn't find a parent for this property
						// this means it is a concept itself
						// and we should only use table
						colSelector = new QueryColumnSelector(table);
						primFlag = true;
					} else {
						colSelector = new QueryColumnSelector(table + "__" + col);
					}
					qs.addSelector(colSelector);

					// select only non-null values from database
					SimpleQueryFilter nulls = new SimpleQueryFilter(new NounMetadata(colSelector, PixelDataType.COLUMN),
							"!=", new NounMetadata("null", PixelDataType.NULL_VALUE));
					qs.addExplicitFilter(nulls);
					IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
					if (!iterator.hasNext()) {
						// all values are null in this column
						continue;
					}
					StringBuilder sb = new StringBuilder();
					sb.append("rm(result);");
					// write to csv and read into R
					String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/"
							+ Utility.getRandomString(6) + ".tsv";
					String header = engine.getEngineId() + seperator + engine.getEngineName() + seperator + table
							+ seperator + col;
					File newFile = CompareDbSemanticSimiliarity.writeResultToFile(newFileLoc, iterator, header);
					sb.append(RSyntaxHelper.getFReadSyntax(rTempTable, newFile.getAbsolutePath(), "\\t") + "\n");

					// get random subset of column data
					sb.append("if(nrow(" + rTempTable + ") > 15) {");
					sb.append(rTempTable + "<-" + rTempTable + "[sample(nrow(" + rTempTable + "),15),c(");
					sb.append("\"" + engineID + seperator + engineName + seperator + table + seperator + col + "\"");
					sb.append(")];}\n");
					//logger.info("Searching description for: " + engine.getEngineName() + ":::" + table + ":::" + col);

					// execute script to get descriptions for this column
					sb.append(RSyntaxHelper.asDataFrame(rTempTable, rTempTable) + "\n");
					sb.append("semantic_tracking_mgr(" + rTempTable + ",\"dataitem\");\n");
					sb.append("result <- readRDS('dataitem-semantic-history.rds');");

					// get only the row that you are adding to the table
					sb.append("result <- result[(result$ENGINE_ID== \"" + engineID + "\" & result$ENGINE_NAME== \""
							+ engineName + "\" & result$TABLE== \"" + table + "\" & result$COLUMN== \"" + col
							+ "\"),]");
					this.rJavaTranslator.runR(sb.toString());
					newFile.delete();
					descriptions = this.rJavaTranslator.getString("as.character(result[1,\"DESCRIPTION\"])");
					if (descriptions == null) {
						// no results found
						continue;
					}

					// Truncate row lengths to be the appropriate lengths
					// before
					// sending to table
					if (primFlag) {
						logger.info("Found description for: " + engine.getEngineName() + ":::" + table);

					} else {
						logger.info("Found description for: " + engine.getEngineName() + ":::" + table + ":::" + col);
					}
					if (descriptions.length() > 1000) {
						descriptions = descriptions.substring(0, 999);
					}

					// Add a new row into the table with the necessary
					// details
					// We check to see if this table already exists in the
					// endpoint
					Object[] newRow = { engineID, engineName, table, col, descriptions };
					list.add(newRow);
				} else {
					continue;
				}
			}
		}
		logger.info(stepCounter + ". Done");
		stepCounter++;
		
		logger.info(stepCounter + ". Storing descriptions");
		sendTrackRequest("semantic", list);
		logger.info(stepCounter + ". Done");
		

		String gc = "rm(\"a5_97b6491748854929b50f55f5818b1634\",	\"a9_8ca904d356784e2d88427675e946b591\","
				+ "\"apply_tfidf\",                        \"assign_unique_concepts\",            "
				+ "\"aTempInsightNotStored\",              \"blend_mgr\",                         "
				+ "\"blend_tracking_semantic\",            \"breakdown\",                         "
				+ "\"build_data_landmarks\",               \"build_dbid_domain\",                 "
				+ "\"build_query_doc\",                    \"build_query_tdm\",                   "
				+ "\"build_sim\",                          \"build_tdm\",                         "
				+ "\"col2db\",                             \"col2tbl\",                           "
				+ "\"column_doc_mgr_do\",                  \"column_doc_mgr_dopar\",              "
				+ "\"column_lsi_mgr\",                     \"compute_column_desc_sim\",           "
				+ "\"compute_entity_sim\",                 \"con\",                               "
				+ "\"construct_column_doc\",               \"constructName\",                     "
				+ "\"cosine_jaccard_sim\",                 \"create_column_doc\",                 "
				+ "\"data_domain_mgr\",                    \"dataitem_history_do\",               "
				+ "\"dataitem_history_dopar\",             \"dataitem_recom_mgr\",                "
				+ "\"datasemantic_history\",               \"discover_column_desc\",              "
				+ "\"drilldown_communities\",              \"exec_tfidf\",                        "
				+ "\"find_db\",                            \"get_dataitem_rating\",               "
				+ "\"get_item_recom\",                     \"get_items_users\",                   "
				+ "\"get_similar_doc\",                    \"get_user_recom\",                    "
				+ "\"getSearchURL\",                       \"hop_away_mgr\",                      "
				+ "\"hop_away_recom_mgr\",                 \"jaccard_sim\",                       "
				+ "\"locate_data_communities\",            \"locate_data_district\",              "
				+ "\"locate_user_communities\",            \"lsi_mgr\",                           "
				+ "\"match_desc\",                         \"populate_ratings\",                  "
				+ "\"read_datamatrix\",                    \"refresh_base\",                      "
				+ "\"refresh_data_mgr\",                   \"refresh_semantic_mgr\",              "
				+ "\"remove_files\",                       \"semantic_tracking_mgr\",             "
				+ "\"semanticTempTable\")";
		this.rJavaTranslator.runR(gc);
	}

	private void sendTrackRequest(String type, List<Object[]> rows) {
		TrackRequestThread t = new TrackRequestThread(type, rows);
		t.start();
	}
}
