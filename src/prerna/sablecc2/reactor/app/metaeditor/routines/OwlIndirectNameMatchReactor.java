package prerna.sablecc2.reactor.app.metaeditor.routines;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;

public class OwlIndirectNameMatchReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = OwlIndirectNameMatchReactor.class.getName();

	/**
	 * Example script to run:
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlColumnNamesFuzzyMatch.R");
	 allTables <- c('city','city','city','city','city','countrylanguage','countrylanguage','countrylanguage','countrylanguage','country','country','country','country','country','country','country','country','country','country','country','country','country','country','country');
	 allColumns <- c('ID','Name','CountryCode','District','Population','CountryCode','Language','IsOfficial','Percentage','Code','Name','Continent','Region','SurfaceArea','IndepYear','Population','LifeExpectancy','GNP','GNPOld','LocalName','GovernmentForm','HeadOfState','Capital','Code2');
	 matches_awiHmTT<- getColumnFuzzyMatches(allTables_aQC7ep4,allColumns_aHGRoJ8);
	 
	 * 
	 */
	
	public OwlIndirectNameMatchReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), TABLES_FILTER};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = getAppId(appId, false);
		List<String> filters = getTableFilters();

		// make sure R is good to go
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR(); 
		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		rJavaTranslator.checkPackages(packages);
		
		IEngine app = Utility.getEngine(appId);
		
		// get 2 lists
		// of all table names
		// and column names
		// matched by index
		List<String>[] lists = getTablesAndColumnsList(app, filters);
		List<String> tableNamesList = lists[0];
		List<String> columnNamesList = lists[1];
		
		StringBuilder script = new StringBuilder();
		
		// first source the file where we have the main method for running
		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlColumnNamesFuzzyMatch.R"; 
		rScriptPath = rScriptPath.replace("\\", "/");
		script.append("source(\"" + rScriptPath + "\");");
		
		script.append("library(stringdist);");
		script.append("library(data.table);");
		
		// need to get all the tables
		// and all the columns
		// this is required for joining back
		String allTablesVar = "allTables_" + Utility.getRandomString(6);
		script.append(allTablesVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(tableNamesList)).append(";");
		// now repeat for columns
		String allColumnsVar = "allColumns_" + Utility.getRandomString(6);
		script.append(allColumnsVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(columnNamesList)).append(";");

		// now that we have defined the inputs, just need to run the "main" method of the script
		String matchDataFrame = "matches_" + Utility.getRandomString(6);
		script.append(matchDataFrame).append( "<- getColumnFuzzyMatches(").append(allTablesVar).append(",").append(allColumnsVar).append(");");
		
		// execute!
		logger.info("Running script to determine distance among column headers..");
		rJavaTranslator.runR(script.toString());
		logger.info("Finished running script!");

		// recreate a new frame and set the frame name
		String[] colNames = new String[]{"sourceCol", "targetCol", "distance", "sourceTable", "targetTable"};
		String[] colTypes = new String[]{"character", "character", "numeric", "character", "character"};

		RDataTable frame = new RDataTable(this.insight.getRJavaTranslator(logger), matchDataFrame);
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(frame, colNames, colTypes, matchDataFrame);
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);		
	
		// store in insight
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(frame);
		}
		this.insight.getVarStore().put(frame.getName(), retNoun);
		
		// return the frame
		return retNoun;
	}
}
