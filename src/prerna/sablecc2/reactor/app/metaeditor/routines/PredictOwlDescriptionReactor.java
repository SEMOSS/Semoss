package prerna.sablecc2.reactor.app.metaeditor.routines;

import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.metaeditor.AbstractMetaEditorReactor;
import prerna.util.Utility;
import prerna.wikidata.WikiDescriptionExtractor;

public class PredictOwlDescriptionReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = PredictOwlDescriptionReactor.class.getName();
	
	/**
	 * 
	 * Example script
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlPredictDescriptions.R");
	 instanceValues_aIPO75a <- c("Classic Cars","Motorcycles","Planes","Ships","Trains");
	 descriptionsFrame_a9L7hr1 <- predictDescriptions(instanceValues_aIPO75a);
	 
	 * 
	 */
	
	public PredictOwlDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		List<String> values = new Vector<String>();
		
		// make sure R is good to go
//		Logger logger = getLogger(CLASS_NAME);
//		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
//		rJavaTranslator.startR(); 
//		// check if packages are installed
//		String[] packages = { "WikidataR", "XML", "RCurl", "stringr"};
//		rJavaTranslator.checkPackages(packages);
		
		String appId = getAppId();
		// we may have an alias
		appId = testAppId(appId, true);
		
		String physicalUri = null;
		String concept = getConcept();
		String prop = getProperty();
		
		IEngine engine = Utility.getEngine(appId);
		SemossDataType dataType = null;
		String qsName = null;
		if(prop == null || prop.isEmpty()) {
			values.add(concept);
			qsName = concept;
		} else {
			values.add(prop);
			qsName = concept + "__" + prop;
		}
		
		physicalUri = engine.getPhysicalUriFromPixelSelector(qsName);
		dataType = SemossDataType.convertStringToDataType(engine.getDataTypes(physicalUri));
		
		Set<String> logicalNames = engine.getLogicalNames(physicalUri);
		values.addAll(logicalNames);
		
		if(dataType == SemossDataType.STRING) {
			logger.info("Grabbing most popular instances to use for searching...");
			IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getMostOccuringSingleColumnNonEmptyQs(qsName, 10));
			while(wrapper.hasNext()) {
				Object value = wrapper.next().getValues()[0];
				if(value == null || value.toString().isEmpty()) {
					continue;
				}
				values.add(value.toString());
			}
			logger.info("Done grabbing must popular instances");
		}
		
		List<String> descriptions = new Vector<String>();
		WikiDescriptionExtractor extractor = new WikiDescriptionExtractor();
		extractor.setLogger(logger);
		for(String value : values) {
			try {
				descriptions.addAll(extractor.getDescriptions(value));
			} catch (Exception e) {
				logger.info("ERROR ::: Could not process input = " + value);
				e.printStackTrace();
			}
		}
		int numDescriptions = descriptions.size();
		if(numDescriptions == 0) {
			throw new SemossPixelException(new NounMetadata("Unable to generate a description for the input column", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		StringBuilder masterDescription = new StringBuilder();
		for(int i = 0; i < numDescriptions; i++) {
			masterDescription.append(descriptions.get(i));
			if((i+1) < numDescriptions) {
				masterDescription.append("\n");
			}
		}
		
		
//		StringBuilder script = new StringBuilder();
//		// first source the file where we have the main method for running
//		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlPredictDescriptions.R"; 
//		rScriptPath = rScriptPath.replace("\\", "/");
//		script.append("source(\"" + rScriptPath + "\");");
//		
//		String descriptionsFrameVar = "descriptionsFrame_" + Utility.getRandomString(6);
//		String instanceVectorVar = "instanceValues_" + Utility.getRandomString(6);
//		script.append(instanceVectorVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(values)).append(";");
//		script.append(descriptionsFrameVar).append(" <- predictDescriptions(").append(instanceVectorVar).append(");");
//
//		// execute!
//		logger.info("Running script to auto generate descriptions...");
//		rJavaTranslator.runR(script.toString());
//		logger.info("Finished running scripts!");
//
//		String[] descriptionValues = rJavaTranslator.getStringArray(descriptionsFrameVar);
//
//		// try to get rid of the duplications
//		Set<String> uniqueDescriptions = new HashSet<String>();
//		for(String s : descriptionValues) {
//			if(s.trim().isEmpty()) {
//				continue;
//			}
//			String[] newSplit = s.trim().split("\\*\\*\\*NEW LINE\\*\\*\\*");
//			for(String split : newSplit) {
//				uniqueDescriptions.add(split);
//			}
//		}
//		
//		StringBuilder masterDescription = new StringBuilder();
//		for(String uniqueVal : uniqueDescriptions) {
//			masterDescription.append(uniqueVal).append(". ");
//		}
		
//		OWLER owler = getOWLER(appId);
//		for(String desc : descriptionValues) {
//			desc = desc.replace("***NEW LINE***", "\n");
//			if(prop == null || prop.isEmpty()) {
//				owler.addConceptDescription(concept, prop, desc);
//			} else {
//				owler.addPropDescription(concept, prop, desc);
//			}
//		}
//		owler.commit();
//		
//		try {
//			owler.export();
//		} catch (IOException e) {
//			e.printStackTrace();
//			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
//			noun.addAdditionalReturn(new NounMetadata("An error occured attempting to add descriptions", 
//					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//			return noun;
//		}
		
		String description = masterDescription.toString();
		if(description.isEmpty()) {
			throw new SemossPixelException(new NounMetadata("Unable to generate a description for the input column", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		NounMetadata noun = new NounMetadata(new String[]{description}, PixelDataType.CONST_STRING);
		noun.addAdditionalReturn(new NounMetadata("Predicted descriptions for review",
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
