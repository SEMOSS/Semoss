package prerna.sablecc2.reactor.app.metaeditor;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.ds.r.RDataTable;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;

public class OwlIndirectNameMatchReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = OwlIndirectNameMatchReactor.class.getName();

	/*
	 * This reactor get the columns that match between all the tables
	 * 
	 * Here is an example script that you would expect from this routine:

			library(stringdist);
			library(data.table);
			
			# get all the columns and tables in 2 lists matched by index
			allTables_aeBUZ8K <- c('city','city','city','city','city','countrylanguage','countrylanguage','countrylanguage','countrylanguage','country','country','country','country','country','country','country','country','country','country','country','country','country','country','country');
			allColumns_aBRGQIz <- c('ID','Name','CountryCode','District','Population','CountryCode','Language','IsOfficial','Percentage','Code','Name','Continent','Region','SurfaceArea','IndepYear','Population','LifeExpectancy','GNP','GNPOld','LocalName','GovernmentForm','HeadOfState','Capital','Code2');
			
			# get all the unique column values
			uniqueColNames_atJfEgY <- unique(allColumns_aBRGQIz);
			# compare all the unique values to themselves
			# this is a matrix with the distance values
			aLdvslD <-stringdistmatrix(uniqueColNames_atJfEgY,uniqueColNames_atJfEgY, method="jw", p=0.1);
			
			# we will switch together the starting frame
			# that contains sourceColumn, sourceTable, and distance
			aQlyW8E <- dim(aLdvslD);
			distance_axvuZaN <- round(as.vector(aLdvslD), 4);
			aH92GSs <- rep(uniqueColNames_atJfEgY, each=aQlyW8E[2]);
			a4SP43L <- rep(uniqueColNames_atJfEgY, aQlyW8E[1]);
			matches_a4mfL5c <- as.data.table(as.data.frame(cbind(aH92GSs,a4SP43L,distance_axvuZaN)));

			# convert the distance to a number and rename
			matches_a4mfL5c[,3] <- as.numeric(as.character(matches_a4mfL5c$distance_axvuZaN));
			names(matches_a4mfL5c) <- c('sourceCol', 'targetCol', 'distance');
			
			# generate a new table so we can join back the tables into the frame
			tableToCol_aDxlJ5W <- data.table(allTables_aeBUZ8K, allColumns_aBRGQIz);
			names(tableToCol_aDxlJ5W) <- c('table', 'column');
			
			# start the merge and rename as we add each table column
			matches_a4mfL5c <- merge(matches_a4mfL5c,tableToCol_aDxlJ5W,by.x='sourceCol', by.y='column', allow.cartesian=TRUE);
			names(matches_a4mfL5c) <- c('sourceCol', 'targetCol', 'distance', 'sourceTable');
			matches_a4mfL5c <- merge(matches_a4mfL5c,tableToCol_aDxlJ5W,by.x='targetCol', by.y='column', allow.cartesian=TRUE);
			names(matches_a4mfL5c) <- c('sourceCol', 'targetCol', 'distance', 'sourceTable', 'targetTable');
			
			# we do not want inner table joins
			# so we will drop those
			matches_a4mfL5c <- matches_a4mfL5c[targetTable != sourceTable];
			
			function_remove_inverts <- function(dt) {
			  for(i in dim(dt[1])) {
			    dt <- dt[dt[, dt$sourceTable[i] == dt$targetTable 
			                  & dt$targetTable[i] == dt$sourceTable
			                  & dt$sourceCol[i] == dt$targetCol 
			                  & dt$targetCol[i] == dt$sourceCol
			                ]]
			  }
			}
			
	 * 
	 */
	
	public OwlIndirectNameMatchReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), "dist"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		appId = getAppId(appId);

		// make sure R is good to go
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR(); 
		// check if packages are installed
		String[] packages = { "stringdist", "data.table" };
		rJavaTranslator.checkPackages(packages);
		
		IEngine app = Utility.getEngine(appId);
		
		// store 2 lists
		// of all table names
		// and column names
		// matched by index
		List<String> tableNamesList = new Vector<String>();
		List<String> columnNamesList = new Vector<String>();
		
		Vector<String> concepts = app.getConcepts(false);
		for(String cUri : concepts) {
			String tableName = Utility.getInstanceName(cUri);
			String tablePrimCol = Utility.getClassName(cUri);
			
			tableNamesList.add(tableName);
			columnNamesList.add(tablePrimCol);
			
			// grab all the properties
			List<String> properties = app.getProperties4Concept(cUri, false);
			for(String pUri : properties) {
				tableNamesList.add(tableName);
				columnNamesList.add(Utility.getClassName(pUri));
			}
		}
		
		StringBuilder script = new StringBuilder();
		script.append("library(stringdist);");
		script.append("library(data.table);");
		
		// need to get all the tables
		// and all the columns
		// this is required for joining back
		String allTablesVar = "allTables_" + Utility.getRandomString(6);
		script.append(allTablesVar).append(" <- c(");
		{
			Iterator<String> it = tableNamesList.iterator();
			if(it.hasNext()) {
				script.append("'").append(it.next()).append("'");
			}
			while(it.hasNext()) {
				script.append(",'").append(it.next()).append("'");
			}
		}
		script.append(");");
		// now repeat for columns
		String allColumnsVar = "allColumns_" + Utility.getRandomString(6);
		script.append(allColumnsVar).append(" <- c(");
		{
			Iterator<String> it = columnNamesList.iterator();
			if(it.hasNext()) {
				script.append("'").append(it.next()).append("'");
			}
			while(it.hasNext()) {
				script.append(",'").append(it.next()).append("'");
			}
		}
		script.append(");");

		// get all the unique columns as well
		String uniqueColumnNamesVar = "uniqueColNames_" + Utility.getRandomString(6);
		script.append(uniqueColumnNamesVar).append(" <- unique(").append(allColumnsVar).append(");");

		String stringMatchVariable = Utility.getRandomString(6);
		script.append(stringMatchVariable).append(" <-stringdistmatrix(")
				.append(uniqueColumnNamesVar)
				.append(",")
				.append(uniqueColumnNamesVar)
				.append(", method=\"jw\", p=0.1);");
		
		String matchDataFrame = "matches_" + Utility.getRandomString(6);
		String size = Utility.getRandomString(6);
		String distanceCol = "distance_" + Utility.getRandomString(6);
		String colnames1 = Utility.getRandomString(6);
		String colnames2 = Utility.getRandomString(6);

		// get the dimension
		script.append(size).append(" <- dim(").append(stringMatchVariable).append(");");
		// flatten the distance matrix
		script.append(distanceCol).append(" <- ").append("round(as.vector(").append(stringMatchVariable).append("), 4);");
		// match the column names with the matrix
		// one will repeat each entity x times before going to the next
		// the other will repeat the vector multiple times
		script.append(colnames1).append(" <- ").append("rep(").append(uniqueColumnNamesVar).append(", each=").append(size).append("[2]);");
		script.append(colnames2).append(" <- ").append("rep(").append(uniqueColumnNamesVar).append(", ").append(size).append("[1]);");

		// construct the frame
		script.append(matchDataFrame).append(" <- as.data.table(as.data.frame(cbind(")
				.append(colnames1)
				.append(",")
				.append(colnames2)
				.append(",")
				.append(distanceCol)
				.append(")));")
				;
		
		// convert the distance column to numeric
		script.append(matchDataFrame).append("[,3] <- as.numeric(as.character(").append(matchDataFrame).append("$").append(distanceCol).append("));");
		script.append("names(").append(matchDataFrame).append(") <- c('sourceCol', 'targetCol', 'distance');");

		// we have created now a frame
		// that has sourceCol, targetCol, distance
		// now we need to join back all the tables for column1, and column2
		// this is done by creating a new frame and then doing a merge
		String tableToColVar = "tableToCol_" + Utility.getRandomString(6);
		script.append(tableToColVar).append(" <- data.table(").append(allTablesVar).append(", ").append(allColumnsVar).append(");");
		script.append("names(").append(tableToColVar).append(") <- c('table', 'column');");

		// now we need to merge
		script.append(matchDataFrame).append(" <- merge(")
				.append(matchDataFrame)
				.append(",")
				.append(tableToColVar)
				.append(",by.x='sourceCol', by.y='column', allow.cartesian=TRUE);")
				;
		// rename
		script.append("names(").append(matchDataFrame).append(") <- c('sourceCol', 'targetCol', 'distance', 'sourceTable');");
		// merge again, but this time on targetCol
		script.append(matchDataFrame).append(" <- merge(")
				.append(matchDataFrame)
				.append(",")
				.append(tableToColVar)
				.append(",by.x='targetCol', by.y='column', allow.cartesian=TRUE);")
				;
		// rename
		script.append("names(").append(matchDataFrame).append(") <- c('sourceCol', 'targetCol', 'distance', 'sourceTable', 'targetTable');");
		
		// we shouldn't have self joins
		// so we will drop columns where table name is the same as both source and target
		script.append(matchDataFrame).append(" <- ").append(matchDataFrame).append("[targetTable != sourceTable];");
		
		// remove unwanted vars
		script.append("rm(")
			.append(allTablesVar)
			.append(",")
			.append(allColumnsVar)
			.append(",")
			.append(uniqueColumnNamesVar)
			.append(",")
			.append(tableToColVar)
			.append(",")
			.append(size)
			.append(",")
			.append(colnames1)
			.append(",")
			.append(colnames2)
			.append(",")
			.append(distanceCol)
			.append(");gc()");
			;
		
		// execute!
		logger.info("Running script to determine distance among column headers..");
		rJavaTranslator.runR(script.toString());
		logger.info("Finished running script!");

		// recreate a new frame and set the frame name
		String[] colNames = new String[]{"sourceCol", "targetCol", "distance", "sourceTable", "targetTable"};
		String[] colTypes = new String[]{"character", "character", "numeric", "character", "character"};

		VarStore vars = this.insight.getVarStore();
		RDataTable frame = null;
		if (vars.get(IRJavaTranslator.R_CONN) != null && vars.get(IRJavaTranslator.R_PORT) != null) {
			frame = new RDataTable(matchDataFrame, 
					(RConnection) vars.get(IRJavaTranslator.R_CONN).getValue(), 
					(String) vars.get(IRJavaTranslator.R_PORT).getValue());
		} else {
			frame = new RDataTable(matchDataFrame);
		}
		ImportUtility.parserRTableColumnsAndTypesToFlatTable(frame, colNames, colTypes, matchDataFrame);
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);		
	
		// store in insight
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(frame);
		}
		this.insight.getVarStore().put(frame.getTableName(), retNoun);
		
		// return the frame
		return retNoun;
	}
}
