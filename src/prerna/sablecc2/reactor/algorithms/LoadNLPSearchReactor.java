package prerna.sablecc2.reactor.algorithms;

import java.io.File;
import java.util.List;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.ds.r.RSyntaxHelper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.AssetUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class LoadNLPSearchReactor extends AbstractRFrameReactor {
	
	protected static final String CLASS_NAME = LoadNLPSearchReactor.class.getName();
	
	protected static final String NLDR_DB = "nldr_db";
	protected static final String NLDR_JOINS = "nldr_joins";
	protected static final String NLDR_MEMBERSHIP = "nldr_membership";
	
	public LoadNLPSearchReactor() {
		this.keysToGet = new String[] { };
	}
	
	@Override
	public NounMetadata execute() {
		this.init();
		this.organizeKeys();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

        String[] packages = new String[] { "igraph", "SteinerNet", "data.table" , "tools" };
		this.rJavaTranslator.checkPackages(packages);
		this.rJavaTranslator.runR(RSyntaxHelper.loadPackages(packages));
		
		// Pull necessary files / create file paths. 
		String savePath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts";
		User user = this.insight.getUser();
		String assetId = user.getAssetProjectId(user.getPrimaryLogin());
		if (assetId != null && !(assetId.isEmpty())) {
			ClusterUtil.pullUserWorkspace(assetId, true);
			savePath = AssetUtility.getUserAssetAndWorkspaceVersionFolder("Asset", assetId) + DIR_SEPARATOR + "assets";

			File assetDir = new File(savePath);
			if (!assetDir.isDirectory() || !assetDir.exists()) {
				assetDir.mkdirs();
			} 
		}
		savePath = savePath.replace("\\", "/"); 

		// source the proper script
		StringBuilder sb = new StringBuilder();
		String wd = "wd" + Utility.getRandomString(5);
		String rFolderPath = baseFolder + DIR_SEPARATOR + "R" + DIR_SEPARATOR + "AnalyticsRoutineScripts" + DIR_SEPARATOR;
		sb.append(wd + "<- getwd();");
		sb.append(("setwd(\"" + savePath + "\");").replace("\\", "/"));
		sb.append(("source(\"" + rFolderPath + "template_assembly.R" + "\");").replace("\\", "/"));
		sb.append(("source(\"" + rFolderPath + "template_db.R" + "\");").replace("\\", "/"));

		
		this.rJavaTranslator.runR(sb.toString());
		
		String nldrPath1 = savePath + DIR_SEPARATOR + "nldr_membership.rds";
		String nldrPath2 = savePath + DIR_SEPARATOR + "nldr_db.rds";
		String nldrPath3 = savePath + DIR_SEPARATOR + "nldr_joins.rds";	
		File nldrMembership = new File(nldrPath1);
		File nldrDb = new File(nldrPath2);
		File nldrJoins = new File(nldrPath3);
		long replaceTime = System.currentTimeMillis() - ((long)1 * 24 * 60 * 60 * 1000);
		
		StringBuilder script = new StringBuilder();
		if(!nldrDb.exists() || !nldrJoins.exists() || !nldrMembership.exists() || nldrMembership.lastModified() < replaceTime ) {
			createRdsFiles(rFolderPath);
		} else {
			script.append(NLDR_MEMBERSHIP + " <- readRDS(\"nldr_membership.rds\");")
				  .append(NLDR_DB + " <- readRDS(\"nldr_db.rds\");")
				  .append(NLDR_JOINS + " <- readRDS(\"nldr_joins.rds\")");
			this.rJavaTranslator.runR(script.toString());
		}
		
		this.rJavaTranslator.runR("setwd(" + wd + ")");
		this.rJavaTranslator.executeEmptyR("rm( " + wd + "); gc();");
		
		// push Asset folder
		if (assetId != null && !(assetId.isEmpty())) {
			ClusterUtil.pushUserWorkspace(assetId, true);
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	/**
	 * Creates the RDS files used for NLP Search
	 */
	private void createRdsFiles(String rFolderPath) {
		StringBuilder sessionTableBuilder = new StringBuilder();

		// source the files
		sessionTableBuilder.append("source(\"" + rFolderPath.replace("\\", "/") + "data_inquiry_guide.R\");");
		sessionTableBuilder.append("source(\"" + rFolderPath.replace("\\", "/") + "data_inquiry_assembly.R\");");

		// use all the databases
		List<String> engineFilters = SecurityEngineUtils.getFullUserDatabaseIds(this.insight.getUser());
		// first get the total number of cols and relationships
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(engineFilters);
		List<String[]> allRelations = MasterDatabaseUtility.getRelationships(engineFilters);
		int totalNumRels = allRelations.size();
		int totalColCount = allTableCols.size();

		// start building script
		String rDatabaseIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		String rColTypes = "c(";
		String rPrimKey = "c(";

		// create R vector of dbId, tables, and columns
		for (int i = 0; i < totalColCount; i++) {
			Object[] entry = allTableCols.get(i);
			String databaseId = entry[0].toString();
			String table = entry[1].toString();
			if (entry[0] != null && entry[1] != null && entry[2] != null && entry[3] != null && entry[4] != null) {
				String column = entry[2].toString();
				String dataType = entry[3].toString();
				String pk = entry[4].toString().toUpperCase();

				if (i == 0) {
					rDatabaseIds += "'" + databaseId + "'";
					rTableNames += "'" + databaseId + "._." + table + "'";
					rColNames += "'" + column + "'";
					rColTypes += "'" + dataType + "'";
					rPrimKey += "'" + pk + "'";
				} else {
					rDatabaseIds += ",'" + databaseId + "'";
					rTableNames += ",'" + databaseId + "._." + table + "'";
					rColNames += ",'" + column + "'";
					rColTypes += ",'" + dataType + "'";
					rPrimKey += ",'" + pk + "'";
				}
			}
		}

		// create R vector of table columns and table rows
		String rDatabaseIdsJoin = "c(";
		String rTbl1 = "c(";
		String rTbl2 = "c(";
		String rJoinBy1 = "c(";
		String rJoinBy2 = "c(";

		int firstRel = 0;
		for (int i = 0; i < totalNumRels; i++) {
			String[] entry = allRelations.get(i);
			String databaseId = entry[0];
			String rel = entry[3];

			String[] relSplit = rel.split("\\.");
			if (relSplit.length == 4) {
				// this is RDBMS
				String sourceTable = relSplit[0];
				String sourceColumn = relSplit[1];
				String targetTable = relSplit[2];
				String targetColumn = relSplit[3];

				// check by firstRel, not index of for loop
				// loop increments even if relSplit.length != 4
				// whereas firstRel only increases if something is added to frame
				if (firstRel == 0) {
					rDatabaseIdsJoin += "'" + databaseId + "'";
					rTbl1 += "'" + databaseId + "._." + sourceTable + "'";
					rTbl2 += "'" + databaseId + "._." + targetTable + "'";
					rJoinBy1 += "'" + sourceColumn + "'";
					rJoinBy2 += "'" + targetColumn + "'";
				} else {
					rDatabaseIdsJoin += ",'" + databaseId + "'";
					rTbl1 += ",'" + databaseId + "._." + sourceTable + "'";
					rTbl2 += ",'" + databaseId + "._." + targetTable + "'";
					rJoinBy1 += ",'" + sourceColumn + "'";
					rJoinBy2 += ",'" + targetColumn + "'";
				}

				if (sourceColumn.endsWith("_FK")) {
					// if column ends with a _FK, then add it to NaturalLangTable also
					rDatabaseIds += ",'" + databaseId + "'";
					rTableNames += ",'" + databaseId + "._." + sourceTable + "'";
					rColNames += ",'" + sourceColumn + "'";
					rColTypes += ", 'STRING' ";
					rPrimKey += ", 'FALSE' ";
				}
				// no longer adding the first row to this data frame, increment..
				firstRel++;
			} else {
				// this is an RDF or Graph
				String sourceTable = entry[1];
				String sourceColumn = entry[1];
				String targetTable = entry[2];
				String targetColumn = entry[2];
				if (firstRel == 0) {
					rDatabaseIdsJoin += "'" + databaseId + "'";
					rTbl1 += "'" + databaseId + "._." + sourceTable + "'";
					rTbl2 += "'" + databaseId + "._." + targetTable + "'";
					rJoinBy1 += "'" + sourceColumn + "'";
					rJoinBy2 += "'" + targetColumn + "'";
				} else {
					rDatabaseIdsJoin += ",'" + databaseId + "'";
					rTbl1 += ",'" + databaseId + "._." + sourceTable + "'";
					rTbl2 += ",'" + databaseId + "._." + targetTable + "'";
					rJoinBy1 += ",'" + sourceColumn + "'";
					rJoinBy2 += ",'" + targetColumn + "'";
				}
				// no longer adding the first row to this data frame, increment..
				firstRel++;
			}
		}

		// close all the arrays created
		rDatabaseIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rColTypes += ")";
		rPrimKey += ")";
		rDatabaseIdsJoin += ")";
		rTbl1 += ")";
		rTbl2 += ")";
		rJoinBy1 += ")";
		rJoinBy2 += ")";

		// address where there were no rels
		if (totalNumRels == 0) {
			rDatabaseIdsJoin = "character(0)";
			rTbl1 = "character(0)";
			rTbl2 = "character(0)";
			rJoinBy1 = "character(0)";
			rJoinBy2 = "character(0)";
		}

		// create the session tables
		String db = "nldrDb" + Utility.getRandomString(5);
		String joins = "nldrJoins" + Utility.getRandomString(5);
		sessionTableBuilder.append(
				db + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = " + rDatabaseIds
						+ ", Datatype = " + rColTypes + ", Key = " + rPrimKey + ", stringsAsFactors = FALSE);");
		sessionTableBuilder.append(joins + " <- data.frame(tbl1 = " + rTbl1 + " , tbl2 = " + rTbl2 + " , joinby1 = "
				+ rJoinBy1 + " , joinby2 = " + rJoinBy2 + " , AppID = " + rDatabaseIdsJoin +  ",AppID2 = "
				+ rDatabaseIdsJoin +", stringsAsFactors = FALSE);");

		// run the cluster tables function
		sessionTableBuilder.append(NLDR_MEMBERSHIP + " <- cluster_tables(" + db + ","+ joins + ");")
						   .append(NLDR_DB + " <- " + db + ";" +NLDR_JOINS + " <- " + joins + ";")
						   .append("saveRDS(" + NLDR_DB + ",\"nldr_db.rds\");saveRDS(" + NLDR_JOINS + ",\"nldr_joins.rds\");");

		this.rJavaTranslator.runR(sessionTableBuilder.toString());
		
		
		this.rJavaTranslator.executeEmptyR("rm( " + db + "," + joins + " ); gc();");
	}

}
