package prerna.sablecc2.reactor.app.metaeditor.routines;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.imports.ImportUtility;
import prerna.util.Utility;

public class OwlInstanceSemanticCosineSimilarityMatchReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = OwlIndirectNameMatchReactor.class.getName();

	/**
	 * Example script to run:
	 
	source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlInstanceCosineDistance.R");
	allTables <- c("productlines","productlines","productlines","productlines","products","products","products","products","products","products");
	allColumns <- c("productLine","textDescription","htmlDescription","image","productCode","productName","productLine","productScale","productVendor","productDescription");
	sampleInstancesList <- list(c("Classic Cars","Motorcycles","Planes","Ships","Trains"),c("Attention car enthusiasts: Make your wildest car ownership dreams come true. Whether you are looking for classic muscle cars, dream sports cars or movie-inspired miniatures, you will find great choices in this category. These replicas feature superb attention to detail and craftsmanship and offer features such as working steering system, opening forward compartment, opening rear trunk with removable spare wheel, 4-wheel independent spring suspension, and so on. The models range in size from 1:10 to 1:24 scale and include numerous limited edition and several out-of-production vehicles. All models include a certificate of authenticity from their manufacturers and come fully assembled and ready for display in the home or office.","Our motorcycles are state of the art replicas of classic as well as contemporary motorcycle legends such as Harley Davidson, Ducati and Vespa. Models contain stunning details such as official logos, rotating wheels, working kickstand, front suspension, gear-shift lever, footbrake lever, and drive chain. Materials used include diecast and plastic. The models range in size from 1:10 to 1:50 scale and include numerous limited edition and several out-of-production vehicles. All models come fully assembled and ready for display in the home or office. Most include a certificate of authenticity.","Unique, diecast airplane and helicopter replicas suitable for collections, as well as home, office or classroom decorations. Models contain stunning details such as official logos and insignias, rotating jet engines and propellers, retractable wheels, and so on. Most come fully assembled and with a certificate of authenticity from their manufacturers.","The perfect holiday or anniversary gift for executives, clients, friends, and family. These handcrafted model ships are unique, stunning works of art that will be treasured for generations! They come fully assembled and ready for display in the home or office. We guarantee the highest quality, and best value.","Model trains are a rewarding hobby for enthusiasts of all ages. Whether you're looking for collectible wooden trains, electric streetcars or locomotives, you'll find a number of great choices for any budget within this category. The interactive aspect of trains makes toy trains perfect for young children. The wooden train sets are ideal for children under the age of 5."),c(),c(),c("S10_1949","S10_4757","S10_4962","S12_1099","S12_1108"),c("1969 Harley Davidson Ultimate Chopper","1952 Alpine Renault 1300","1996 Moto Guzzi 1100i","2003 Harley-Davidson Eagle Drag Bike","1972 Alfa Romeo GTA"),c("Classic Cars","Motorcycles","Planes","Ships","Trains"),c("1:10","1:12","1:18","1:72","1:24"),c("Min Lin Diecast","Classic Metal Creations","Highway 66 Mini Classics","Red Start Diecast","Motor City Art Classics"),c("This replica features working kickstand, front suspension, gear-shift lever, footbrake lever, drive chain, wheels and steering. All parts are particularly delicate due to their precise scale and require special care and attention.","Turnable front wheels; steering function; detailed interior; detailed engine; opening hood; opening trunk; opening doors; and detailed chassis.","Official Moto Guzzi logos and insignias, saddle bags located on side of motorcycle, detailed engine, working steering, working suspension, two leather seats, luggage rack, dual exhaust pipes, small saddle bag located on handle bars, two-tone paint with chrome accents, superior die-cast detail , rotating wheels , working kick stand, diecast metal with plastic parts and baked enamel finish.","Model features, official Harley Davidson logos and insignias, detachable rear wheelie bar, heavy diecast metal with resin parts, authentic multi-color tampo-printed graphics, separate engine drive belts, free-turning front fork, rotating tires and rear racing slick, certificate of authenticity, detailed engine, display stand
	, precision diecast replica, baked enamel finish, 1:10 scale model, removable fender, seat and tank cover piece for displaying the superior detail of the v-twin engine","Features include: Turnable front wheels; steering function; detailed interior; detailed engine; opening hood; opening trunk; opening doors; and detailed chassis."));
	
	matches_aynUtlg<- getDocumentCosineSimilarity(allTables,allColumns,sampleInstancesList);	 
	 
	 * 
	 */
	
	public OwlInstanceSemanticCosineSimilarityMatchReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), TABLES_FILTER, STORE_VALUES_FRAME};
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
		String[] packages = { "text2vec", "data.table", "lsa", "WikidataR", "XML", "RCurl", "stringr", "httr"};
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
			
			// if this is empty
			// no filters have been defined
			if(!filters.isEmpty()) {
				// filters have been defined
				// now if the table isn't included
				// ignore it
				if(!filters.contains(tableName)) {
					continue;
				}
			}
			
			String tablePrimCol = Utility.getClassName(cUri);
			
			// we will only store string values!!!
			SemossDataType cDataType = SemossDataType.convertStringToDataType(app.getDataTypes(cUri).replace("TYPE:", ""));
			if(cDataType == SemossDataType.STRING) {
				tableNamesList.add(tableName);
				columnNamesList.add(tablePrimCol);
				
				List<String> colValues = new Vector<String>();
				IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(app, getSingleColumnNonEmptyQs(tableName, 5));
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
					IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(app, getSingleColumnNonEmptyQs(tableName + "__" + colName, 5));
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
		script.append(matchDataFrame).append( "<- getDocumentCosineSimilarity(").append(allTablesVar).append(",").append(allColumnsVar).append(",").append(isntanceListVar).append(");");

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
