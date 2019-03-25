package prerna.sablecc2.reactor.app.metaeditor.routines;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.Utility;
import prerna.wikidata.WikiLogicalNameExtractor;

public class PredictOwlLogicalNamesReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = PredictOwlLogicalNamesReactor.class.getName();
	
	/**
	 * 
	 * Example script
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlPredictLogicalNames.R");
	 instanceValues_aIPO75a <- c("Classic Cars","Motorcycles","Planes","Ships","Trains");
	 descriptionsFrame_a9L7hr1 <- predictLogicalNames(instanceValues_aIPO75a);
	 
	 * 
	 */
	
	public PredictOwlLogicalNamesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		List<String> values = new Vector<String>();

		// make sure R is good to go
//		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
//		rJavaTranslator.startR(); 
//		// check if packages are installed
//		String[] packages = { "WikidataR", "XML", "RCurl", "stringr"};
//		rJavaTranslator.checkPackages(packages);
		
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		
		IEngine engine = Utility.getEngine(appId);
		SemossDataType dataType = null;
		String qsName = null;
		if(prop == null || prop.isEmpty()) {
			values.add(concept);
			qsName = concept;
			dataType = SemossDataType.convertStringToDataType(MasterDatabaseUtility.getBasicDataType(appId, qsName, null));
		} else {
			values.add(prop);
			qsName = concept + "__" + prop;
			dataType = SemossDataType.convertStringToDataType(MasterDatabaseUtility.getBasicDataType(appId, prop, concept));
		}
		
		if(dataType != SemossDataType.STRING) {
			throw new SemossPixelException(new NounMetadata("Can only generate descriptions on string valued input", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getMostOccuringSingleColumnNonEmptyQs(qsName, 5));
		while(wrapper.hasNext()) {
			Object value = wrapper.next().getValues()[0];
			if(value == null || value.toString().isEmpty()) {
				continue;
			}
			values.add(value.toString());
		}
		
		List<String> logicalNames = new Vector<String>();
		WikiLogicalNameExtractor extractor = new WikiLogicalNameExtractor();
		extractor.setLogger(logger);
		for(String value : values) {
			try {
				logicalNames.addAll(extractor.getLogicalNames(value));
			} catch (Exception e) {
				logger.info("ERROR ::: Could not process input = " + value);
				e.printStackTrace();
			}
		}
		// return only the top 5 results
		List<String> topLogicalNames = getTopNResults(logicalNames, 5);
		
//		StringBuilder script = new StringBuilder();
//		// first source the file where we have the main method for running
//		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlPredictLogicalNames.R"; 
//		rScriptPath = rScriptPath.replace("\\", "/");
//		script.append("source(\"" + rScriptPath + "\");");
//		
//		String logicalNamesVar = "logicalFrame_" + Utility.getRandomString(6);
//		String instanceVectorVar = "instanceValues_" + Utility.getRandomString(6);
//		script.append(instanceVectorVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(values)).append(";");
//		script.append(logicalNamesVar).append(" <- predictLogicalNames(").append(instanceVectorVar).append(");");
//
//		// execute!
//		logger.info("Running script to auto generate logical names...");
//		rJavaTranslator.runR(script.toString());
//		logger.info("Finished running scripts!");
//
//		String[] logicalNames = rJavaTranslator.getStringArray(logicalNamesVar);

		NounMetadata noun = new NounMetadata(topLogicalNames, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(new NounMetadata("Predicted and logical names for review",
				PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}

	
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////
	///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
	///////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////

	private String getAppId() {
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			String appId = (String) grs.get(0);
			if (appId != null && !appId.isEmpty()) {
				return appId;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[0]);
	}

	private String getConcept() {
		GenRowStruct grs = this.store.getNoun(keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			String concept = (String) grs.get(0);
			if (concept != null && !concept.isEmpty()) {
				return concept;
			}
		}
		throw new IllegalArgumentException("Need to define " + keysToGet[1]);
	}
	
	private String getProperty() {
		GenRowStruct grs = this.store.getNoun(keysToGet[2]);
		if (grs != null && !grs.isEmpty()) {
			String prop = (String) grs.get(0);
			if (prop != null && !prop.isEmpty()) {
				return prop;
			}
		}

		return "";
	}
}
