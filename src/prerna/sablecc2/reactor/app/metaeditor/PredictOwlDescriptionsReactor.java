package prerna.sablecc2.reactor.app.metaeditor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
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
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public class PredictOwlDescriptionsReactor extends AbstractMetaEditorReactor {

	private static final String CLASS_NAME = PredictOwlDescriptionsReactor.class.getName();
	
	/**
	 * 
	 * Example script
	 
	 source("C:/workspace/Semoss_Dev/R/OwlMatchRoutines/OwlPredictDescriptions.R");
	 instanceValues_aIPO75a <- c("Classic Cars","Motorcycles","Planes","Ships","Trains");
	 descriptionsFrame_a9L7hr1 <- predictDescriptions(instanceValues_aIPO75a);
	 
	 * 
	 */
	
	public PredictOwlDescriptionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONCEPT.getKey(), ReactorKeysEnum.COLUMN.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		// make sure R is good to go
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR(); 
		// check if packages are installed
		String[] packages = { "WikidataR", "XML", "RCurl", "stringr"};
		rJavaTranslator.checkPackages(packages);
		
		String appId = getAppId();
		// we may have an alias
		appId = getAppId(appId, true);
		
		String concept = getConcept();
		String prop = getProperty();
		
		IEngine engine = Utility.getEngine(appId);
		SemossDataType dataType = null;
		String qsName = null;
		if(prop == null || prop.isEmpty()) {
			qsName = concept;
			dataType = SemossDataType.convertStringToDataType(MasterDatabaseUtility.getBasicDataType(appId, qsName, null));
		} else {
			qsName = concept + "__" + prop;
			dataType = SemossDataType.convertStringToDataType(MasterDatabaseUtility.getBasicDataType(appId, prop, concept));
		}
		
		if(dataType != SemossDataType.STRING) {
			throw new SemossPixelException(new NounMetadata("Can only generate descriptions on string valued input", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
		}
		
		List<String> values = new Vector<String>();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, getSingleColumnNonEmptyQs(qsName, 5));
		while(wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0].toString());
		}
		
		StringBuilder script = new StringBuilder();
		// first source the file where we have the main method for running
		String rScriptPath = getBaseFolder() + "\\R\\OwlMatchRoutines\\OwlPredictDescriptions.R"; 
		rScriptPath = rScriptPath.replace("\\", "/");
		script.append("source(\"" + rScriptPath + "\");");
		
		String descriptionsFrameVar = "descriptionsFrame_" + Utility.getRandomString(6);
		String instanceVectorVar = "instanceValues_" + Utility.getRandomString(6);
		script.append(instanceVectorVar).append(" <- ").append(RSyntaxHelper.createStringRColVec(values)).append(";");
		script.append(descriptionsFrameVar).append(" <- predictDescriptions(").append(instanceVectorVar).append(");");

		// execute!
		logger.info("Running script to auto generate descriptions...");
		rJavaTranslator.runR(script.toString());
		logger.info("Finished running scripts!");

		String[] descriptionValues = rJavaTranslator.getStringArray(descriptionsFrameVar);

		// try to get rid of the duplications
		Set<String> uniqueDescriptions = new HashSet<String>();
		for(String s : descriptionValues) {
			String[] newSplit = s.split("\\*\\*\\*NEW LINE\\*\\*\\*");
			for(String split : newSplit) {
				uniqueDescriptions.add(split);
			}
		}
		
		StringBuilder masterDescription = new StringBuilder();
		for(String uniqueVal : uniqueDescriptions) {
			masterDescription.append(uniqueVal).append(". ");
		}
		
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
		
		NounMetadata noun = new NounMetadata(new String[]{masterDescription.toString()}, PixelDataType.CONST_STRING);
//		noun.addAdditionalReturn(new NounMetadata("Predicted and stored descriptions for review",
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
