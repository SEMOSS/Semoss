package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.AddToMasterDB;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.reactor.frame.r.CompareDbSemanticSimiliarity;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.usertracking.UserTrackerFactory;

public class StoreUniqueColumnsReactor extends AbstractRFrameReactor {
	
	private static final String CLASS_NAME = StoreUniqueColumnsReactor.class.getName();
	public static final String DESCRIPTIONS_BOOL = "descriptions";

	public StoreUniqueColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), DESCRIPTIONS_BOOL};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get inputs - engine
		String engineName = this.keyValue.get(this.keysToGet[0]);
		// TODO: bug fix
		engineName = engineName.replace(" ", "_");
		engineName = MasterDatabaseUtility.testEngineIdIfAlias(engineName);
		boolean descriptions = getDescriptionsBool();
		IEngine engine = Utility.getEngine(engineName);

		// validate engine exists
		if (engine == null){
			throw new IllegalArgumentException("Engine doesnt exist");
		}
		String owlPath = engine.getOWL();

		// only executes for rdbms, tinker, and rdf
		ENGINE_TYPE engineType = engine.getEngineType();
		if (engineType.equals(ENGINE_TYPE.RDBMS) || engineType.equals(ENGINE_TYPE.SESAME) || engineType.equals(ENGINE_TYPE.TINKER)) {
			OWLER owl = new OWLER(engine, owlPath);
			owl.addUniqueCounts(engine);
		}
		
		// store descriptions if requested
		if(descriptions){
			storeColumnDescriptions(engine);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private boolean getDescriptionsBool() {
		GenRowStruct boolGrs = this.store.getNoun(DESCRIPTIONS_BOOL);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}
	
	private void storeColumnDescriptions(IEngine engine){
        // source all scripts
        String wd = this.rJavaTranslator.getString("getwd()");
        String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String wkDirSource = "setwd(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\");\n ";
		wkDirSource += "source(\"" + baseFolder + "\\R\\Recommendations\\SemanticSimilarity\\column_doc.r\");\n";
		wkDirSource = wkDirSource.replace("\\", "/");
        this.rJavaTranslator.runR(wkDirSource);
        
		// generate descriptions
        String rTempTable = "semanticTempTable";
		Logger logger = getLogger(CLASS_NAME);
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engine.getEngineId());
		// iterate all columns in db and run R script to generate 
		// description and add to local master index
        String database = engine.getEngineName();
        String seperator = "$";
		HashMap<String,Map> gaData = new HashMap<String, Map>();
		for (Object[] tableCol : allTableCols) {
			SelectQueryStruct qs = new SelectQueryStruct();
			if (tableCol.length == 4) {
				String table = tableCol[0] + "";
				String col = tableCol[1] + "";
				String dataType = tableCol[2] + "";
				boolean primFlag = false;
				String descriptions = "";
				if (dataType.equals(SemossDataType.STRING.toString())) {
					// we will fill this in once we figure out if it is a concept or property
					QueryColumnSelector colSelector = null;
					// this is a hack we used in advanced federate
					if(engine.getParentOfProperty(col + "/" + table) == null) {
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
					SimpleQueryFilter nulls = new SimpleQueryFilter(
							new NounMetadata(colSelector, PixelDataType.COLUMN), "!=",
							new NounMetadata("null", PixelDataType.NULL_VALUE));
					qs.addExplicitFilter(nulls);
					IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
					if(!iterator.hasNext()){
						// all values are null in this column
						continue;
					}
					StringBuilder sb = new StringBuilder();

					// write to csv and read into R
					String newFileLoc = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "/" + Utility.getRandomString(6) + ".tsv";
					File newFile = CompareDbSemanticSimiliarity.writeResultToFile(newFileLoc, iterator, engine.getEngineName() + seperator + table + seperator + col);
					String loadFileRScript = rTempTable + " <- fread(\"" + newFile.getAbsolutePath().replace("\\", "/") + "\", sep=\"\t\");\n";
					sb.append(loadFileRScript);

					// get random subset of column data
					sb.append("if(nrow(" + rTempTable + ") > 12) {");
					sb.append(rTempTable + "<-" + rTempTable + "[sample(nrow(" + rTempTable + "),12),c(");
					sb.append("\"" + database + seperator + table + seperator + col + "\"");
					sb.append(")];}\n");
					logger.debug("Generating and storing descriptions for : " + engine.getEngineName() + ", " + table + ", " + col);
					
					// execute script to get descriptions for this column
					sb.append(rTempTable + "<-as.data.frame(" + rTempTable + ");\n");
					sb.append("result <- column_doc_mgr(" + rTempTable + ",\"column-desc-set\");\n");
					sb.append("result <- paste(result[2][,1], collapse=\" ;;; \")");
					String val = sb.toString();
					this.rJavaTranslator.runR(val);
					newFile.delete();

					// receive triple comma separated descriptions from R
					descriptions = this.rJavaTranslator.getString("result;");
					this.rJavaTranslator.runR("rm(result)");
					if (descriptions == null){
						// no results found
						continue;
					}
				} else {
					// only track string columns
					continue;
				}
				// check if table exists already, add to it, or add it as fresh map
				Map<String, String> tableMap;
				if (gaData.containsKey(table)) {
					// get current map and add to it
					tableMap = gaData.get(table);
				} else {
					tableMap = new HashMap<String, String>();
				}

				// storing the descriptions in local master meta table, once needed
				MasterDatabaseUtility.deleteMetaValue(engine.getEngineId(), col, "SemanticDescription");
				AddToMasterDB master = new AddToMasterDB();
				descriptions = descriptions.replaceAll("'", "");
				master.addMetadata(engine.getEngineId(), col, "SemanticDescription", descriptions);

				col = ((primFlag) ? "PRIM_KEY_PLACEHOLDER" : col);
				tableMap.put(col, descriptions);
				gaData.put(table, tableMap);
			}
		}
		
		UserTrackerFactory.getInstance().trackDescriptions(this.insight, engine.getEngineId(), database, gaData);

		// set working directory back to normal
		this.rJavaTranslator.runR("setwd('" + wd + "');\n");
		
		this.rJavaTranslator.runR("rm(result, apply_tfidf, build_query_tdm, col2db,column_doc_mgr, "
				+ "compute_column_desc_sim, find_columns_bydesc, "
				+ "get_sim_query, lsi_mgr, build_query_doc, build_tdm, col2tbl, "
				+ "column_lsi_mgr, compute_entity_sim, create_column_doc, "
				+ "find_exist_columns_bydesc, get_similar_doc, match_desc, "
				+ "table_lsi_mgr,construct_column_doc, constructName,  discover_column_desc, "
				+ "get_column_desc,get_column_desc_alt, getSearchURL);");
	}
}
