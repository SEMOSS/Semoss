package prerna.reactor.algorithms;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

// RunGPT2Description(descriptionType=["Table"],app=["814690f1-ea53-4179-a526-494d9431def7"],table=["MOVIE"],numDescriptions=[1])
// RunGPT2Description(descriptionType=["App"],app=["814690f1-ea53-4179-a526-494d9431def7"],table=[""],numDescriptions=[1])

public class RunGPT2DescriptionReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = RunGPT2DescriptionReactor.class.getName();
	private static final String DESCRIPTION_TYPE = "descriptionType";
	private static final String NUMBER_DESCRIPTIONS = "numDescriptions";

	public RunGPT2DescriptionReactor() {
		this.keysToGet = new String[] { DESCRIPTION_TYPE, ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.TABLE.getKey() , NUMBER_DESCRIPTIONS };
	}

	@Override
	public NounMetadata execute() {
		// set up the class
		init();
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		StringBuilder rsb = new StringBuilder();

		String[] packages = new String[] { "gpt2" };
		this.rJavaTranslator.checkPackages(packages);
		
		// get inputs
		String descType = getInputString(DESCRIPTION_TYPE);
		String databaseId = getInputString(ReactorKeysEnum.DATABASE.getKey());
		String tableName = getInputString(ReactorKeysEnum.TABLE.getKey());
		int numDescriptions = getInputInt(NUMBER_DESCRIPTIONS);

		// source the files
		String baseFolder = getBaseFolder();
		String source = "source(\"" + baseFolder + "\\R\\AnalyticsRoutineScripts\\proceed.R\");";
		rsb.append(source.replace("\\", "/"));
		
		// get the db table
		String dbTable = getDbTable(databaseId,tableName);
		
		// run the function on either table or database
		String result = "result" + Utility.getRandomString(6);
		String inputVar = "inputVar" + Utility.getRandomString(6);
		if(descType.equals("Table")) {
			rsb.append(inputVar + " <-" + dbTable + "[" + dbTable + "$Table==\"" + databaseId + "._." + tableName + "\",]$Column;");
			rsb.append(result + " <- infer_tbl_desc(" + inputVar + ", qty=" + numDescriptions + ");");
		} else if(descType.equals("App")) {
			rsb.append(inputVar + " <-" + dbTable + "[" + dbTable + "$AppID==\"" + databaseId + "\",];");
			rsb.append(result + " <- infer_db_desc(" + inputVar + ", qty=" + numDescriptions + ");");
		} 
		
		// get the result as a string
		this.rJavaTranslator.runR(rsb.toString());
		String[] resultStrings = this.rJavaTranslator.getStringArray(result);
		
		// gc
		this.rJavaTranslator.executeEmptyR("rm( " + result + "," + inputVar + "," + dbTable + "); gc();");
		
		// return data to the front end as string array
		return new NounMetadata(resultStrings, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

	private String getDbTable(String databaseId, String tableName) {
		StringBuilder sessionTableBuilder = new StringBuilder();

		// first get the total number of cols
		List<Object[]> allTableCols = MasterDatabaseUtility.getAllTablesAndColumns(databaseId);
		int totalColCount = allTableCols.size();

		// start building script
		String rAppIds = "c(";
		String rTableNames = "c(";
		String rColNames = "c(";
		String rColTypes = "c(";
		String rPrimKey = "c(";

		// create R vector of appid, tables, and columns
		for (int i = 0; i < totalColCount; i++) {
			Object[] entry = allTableCols.get(i);
			String table = entry[0].toString();
			
			// if they specified table, make sure it matches
			if (tableName == null || tableName.isEmpty() || table.equals(tableName)) {
				if (entry[0] != null && entry[1] != null && entry[2] != null && entry[3] != null) {
					String column = entry[1].toString();
					String dataType = entry[2].toString();
					String pk = entry[3].toString().toUpperCase();
					if (i == 0) {
						rAppIds += "'" + databaseId + "'";
						rTableNames += "'" + databaseId + "._." + table + "'";
						rColNames += "'" + column + "'";
						rColTypes += "'" + dataType + "'";
						rPrimKey += "'" + pk + "'";
					} else {
						rAppIds += ",'" + databaseId + "'";
						rTableNames += ",'" + databaseId + "._." + table + "'";
						rColNames += ",'" + column + "'";
						rColTypes += ",'" + dataType + "'";
						rPrimKey += ",'" + pk + "'";
					}
				}
			}
		}

		// close all the arrays created
		rAppIds += ")";
		rTableNames += ")";
		rColNames += ")";
		rColTypes += ")";
		rPrimKey += ")";
		
		// create the session tables
		String db = "dbtable" + Utility.getRandomString(5);
		sessionTableBuilder.append(
				db + " <- data.frame(Column = " + rColNames + " , Table = " + rTableNames + " , AppID = " + rAppIds
						+ ", Datatype = " + rColTypes + ", Key = " + rPrimKey + ", stringsAsFactors = FALSE);");
		
		this.rJavaTranslator.runR(sessionTableBuilder.toString());
		
		return db;

	}

	///////////////////// UTILITY /////////////////////////////////////
	private String getInputString(String inputName) {
		GenRowStruct grs = this.store.getNoun(inputName);
		String value = "";
		NounMetadata noun;
		if (grs != null && grs.size() > 0) {
			noun = grs.getNoun(0);
			value = noun.getValue().toString();
		}
		return value;
	}

	private List<String> getInputList(String input) {
		List<String> retList = new Vector<>();
		GenRowStruct engineGrs = this.store.getNoun(input);
		for (int i = 0; i < engineGrs.size(); i++) {
			retList.add(engineGrs.get(i).toString());
		}

		return retList;
	}

	private int getInputInt(String inputName) {
		GenRowStruct grs = this.store.getNoun(inputName);
		int value = -1;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			value = ((Number) noun.getValue()).intValue();
		}
		return value;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(DESCRIPTION_TYPE)) {
			return "Indicate whether you would like to generate a description for one table or an entire database";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
