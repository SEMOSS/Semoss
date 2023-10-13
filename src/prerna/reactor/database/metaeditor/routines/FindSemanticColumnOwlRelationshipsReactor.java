package prerna.reactor.database.metaeditor.routines;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IDatabaseEngine;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class FindSemanticColumnOwlRelationshipsReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = FindSemanticColumnOwlRelationshipsReactor.class.getName();

	/**
	 * Example script to run:
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlColumnCosineDistance.R");
	 allTables <- c('city','city','city','city','city','countrylanguage','countrylanguage','countrylanguage','countrylanguage','country','country','country','country','country','country','country','country','country','country','country','country','country','country','country');
	 allColumns <- c('ID','Name','CountryCode','District','Population','CountryCode','Language','IsOfficial','Percentage','Code','Name','Continent','Region','SurfaceArea','IndepYear','Population','LifeExpectancy','GNP','GNPOld','LocalName','GovernmentForm','HeadOfState','Capital','Code2');
	 matches_awiHmTT<- getDocumentCosineSimilarity(allTables,allColumns);
	 
	 * 
	 */
	
	public FindSemanticColumnOwlRelationshipsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), TABLES_FILTER, STORE_VALUES_FRAME};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = testDatabaseId(databaseId, false);
		List<String> filters = getTableFilters();
		
		// make sure R is good to go
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR(); 
		// check if packages are installed
		String[] packages = { "text2vec", "data.table", "lsa", "WikidataR", "XML", "RCurl", "stringr"};
		rJavaTranslator.checkPackages(packages);
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		
		// get 2 lists
		// of all table names
		// and column names
		// matched by index
		List<String>[] lists = getTablesAndColumnsList(database, filters);
		List<String> tableNamesList = lists[0];
		List<String> columnNamesList = lists[1];
		
		StringBuilder script = new StringBuilder();
		
		// first source the file where we have the main method for running
		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlColumnCosineDistance.R"; 
		rScriptPath = rScriptPath.replace("\\", "/");
		script.append("source(\"" + rScriptPath + "\");");
		
		// need to get all the tables
		// and all the columns
		// this is required for joining back
		String allTablesVar = "allTables_" + Utility.getRandomString(6);
		script.append(allTablesVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(tableNamesList)).append(";");
		// now repeat for columns
		String allColumnsVar = "allColumns_" + Utility.getRandomString(6);
		script.append(allColumnsVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(columnNamesList)).append(";");
		
		// grab existing definitions for the inputs
		// well, this is definitely RDBMS specific so...
		StringBuilder existingDefinitions = null;
		int size = tableNamesList.size();
		for(int i = 0; i < size; i++) {
			String tName = tableNamesList.get(i);
			String cName = columnNamesList.get(i);
			String description = database.getDescription("http://semoss.org/ontologies/Relation/Contains/" + cName + "/" + tName);
			if(description != null && !description.isEmpty()) {
				if(existingDefinitions == null) {
					existingDefinitions = new StringBuilder("list(");
					existingDefinitions.append("c(\"").append(tName)
						.append("\",\"").append(cName)
						// escape quotes
						.append("\",\"").append(description.replace("\"", "\\\""))
						.append("\")");
				} else {
					existingDefinitions.append(", c(\"").append(tName)
					.append("\",\"").append(cName)
					// escape quotes
					.append("\",\"").append(description.replace("\"", "\\\""))
					.append("\")");
				}
			}
		}
		
		// now that we have defined the inputs, just need to run the "main" method of the script
		String matchDataFrame = "matches_" + Utility.getRandomString(6);
		if(existingDefinitions == null) {
			script.append(matchDataFrame).append( "<- getDocumentCosineSimilarity(").append(allTablesVar).append(",").append(allColumnsVar).append(");");
		} else {
			// close the list
			existingDefinitions.append(")");
			script.append(matchDataFrame).append( "<- getDocumentCosineSimilarity(").append(allTablesVar).append(",").append(allColumnsVar)
				.append(",").append(existingDefinitions.toString()).append(");");
		}
		// execute!
		logger.info("Running script to auto generate descriptions...");
		logger.info("Running script to build term document frequency for description similarity...");
		rJavaTranslator.runR(script.toString());
		logger.info("Finished running scripts!");

		// remove subset of stored values
		removeStoredValues(matchDataFrame, new Object[]{"added","removed","auto_added"}, logger);
		
		// recreate a new frame and set the frame name
		String[] colNames = rJavaTranslator.getColumns(matchDataFrame);
		String[] colTypes = rJavaTranslator.getColumnTypes(matchDataFrame);

		RDataTable frame = new RDataTable(this.insight.getRJavaTranslator(logger), matchDataFrame);
		ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(), colNames, colTypes, matchDataFrame);
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
