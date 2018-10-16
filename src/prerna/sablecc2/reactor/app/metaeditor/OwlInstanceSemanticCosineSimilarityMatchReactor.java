package prerna.sablecc2.reactor.app.metaeditor;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.IRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;

public class OwlInstanceSemanticCosineSimilarityMatchReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = OwlIndirectNameMatchReactor.class.getName();

	/**
	 * Example script to run:
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlCosineDistance.R");
	 allTables_aQC7ep4 <- c('city','city','city','city','city','countrylanguage','countrylanguage','countrylanguage','countrylanguage','country','country','country','country','country','country','country','country','country','country','country','country','country','country','country');
	 allColumns_aHGRoJ8 <- c('ID','Name','CountryCode','District','Population','CountryCode','Language','IsOfficial','Percentage','Code','Name','Continent','Region','SurfaceArea','IndepYear','Population','LifeExpectancy','GNP','GNPOld','LocalName','GovernmentForm','HeadOfState','Capital','Code2');
	 matches_awiHmTT<- getDocumentCostineSimilarityMatrix(allTables_aQC7ep4,allColumns_aHGRoJ8);
	 
	 * 
	 */
	
	public OwlInstanceSemanticCosineSimilarityMatchReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
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
		String[] packages = { "text2vec", "data.table", "lsa", "WikidataR" };
		rJavaTranslator.checkPackages(packages);
		
		IEngine app = Utility.getEngine(appId);
		
		// store 2 lists
		// of all table names
		// and column names
		// matched by index
		List<String> tableNamesList = new Vector<String>();
		List<String> columnNamesList = new Vector<String>();
		// also store a matrix of instances
		List<List<String>> sampleInstances = new Vector<List<String>>();
		
		Vector<String> concepts = app.getConcepts(false);
		for(String cUri : concepts) {
			String tableName = Utility.getInstanceName(cUri);
			String tablePrimCol = Utility.getClassName(cUri);
			
			// we will only store string values!!!
			SemossDataType cDataType = SemossDataType.convertStringToDataType(app.getDataTypes(cUri).replace("TYPE:", ""));
			if(cDataType == SemossDataType.STRING) {
				tableNamesList.add(tableName);
				columnNamesList.add(tablePrimCol);
				
				List<String> colValues = new Vector<String>();
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(app, getBaseQs(tableName, 5));
				try {
					while(wrapper.hasNext()) {
						colValues.add(wrapper.next().getValues()[0].toString());
					} 
				} finally {
					wrapper.cleanUp();
				}
				sampleInstances.add(colValues);
			}
				
			// grab all the properties
			List<String> properties = app.getProperties4Concept(cUri, false);
			for(String pUri : properties) {
				// we will only store string values!!!
				SemossDataType pDataType = SemossDataType.convertStringToDataType(app.getDataTypes(pUri).replace("TYPE:", ""));
				if(pDataType == SemossDataType.STRING) {
					tableNamesList.add(tableName);
					String colName = Utility.getClassName(pUri);
					columnNamesList.add(colName);
				
					List<String> colValues = new Vector<String>();
					IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(app, getBaseQs(tableName + "__" + colName, 5));
					try {
						while(wrapper.hasNext()) {
							colValues.add(wrapper.next().getValues()[0].toString());
						} 
					} finally {
						wrapper.cleanUp();
					}
					sampleInstances.add(colValues);
				}
			}
		}
		
		StringBuilder script = new StringBuilder();
		
		// first source the file where we have the main method for running
		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlInstanceCosineDistance.R"; 
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
		
		// will now create a list of vectors for each sample instances we have collected
		String isntanceListVar = "instances_" + Utility.getRandomString(6);
		int numVals = sampleInstances.size();
		script.append(isntanceListVar).append(" <- list(");
		script.append(RSyntaxHelper.createStringRColVec(sampleInstances.get(0)));
		for(int i = 1; i < numVals; i++) {
			script.append(",").append(RSyntaxHelper.createStringRColVec(sampleInstances.get(i)));
		}
		script.append(");");
		// now that we have defined the inputs, just need to run the "main" method of the script
		String matchDataFrame = "matches_" + Utility.getRandomString(6);
		script.append(matchDataFrame).append( "<- getDocumentCostineSimilarityMatrix(").append(allTablesVar).append(",").append(allColumnsVar).append(",").append(isntanceListVar).append(");");

		// execute!
		logger.info("Running script to auto generate descriptions...");
		logger.info("Running script to build term document frequency for description similarity...");
		rJavaTranslator.runR(script.toString());
		logger.info("Finished running scripts!");

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
	
	private SelectQueryStruct getBaseQs(String qsName, int limit) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(qsName));
		qs.setLimit(limit);
		
		{
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(qsName), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata(null, PixelDataType.NULL_VALUE);
			SimpleQueryFilter f = new SimpleQueryFilter(lComparison, "!=", rComparison );
			qs.addExplicitFilter(f);
		}
		{
			NounMetadata lComparison = new NounMetadata(new QueryColumnSelector(qsName), PixelDataType.COLUMN);
			NounMetadata rComparison = new NounMetadata("", PixelDataType.CONST_STRING);
			SimpleQueryFilter f = new SimpleQueryFilter(lComparison, "!=", rComparison );
			qs.addExplicitFilter(f);
		}
		
		return qs;
	}
	
}
