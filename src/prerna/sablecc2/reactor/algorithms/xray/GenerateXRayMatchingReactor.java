package prerna.sablecc2.reactor.algorithms.xray;

import java.util.List;

import org.apache.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateXRayMatchingReactor extends AbstractRFrameReactor {

	public static final String CLASS_NAME = GenerateXRayMatchingReactor.class.getName();
	
	public GenerateXRayMatchingReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), 
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONFIG.getKey(), ReactorKeysEnum.OVERRIDE.getKey(),
				"similarity", "candidate", "matchSameDb"};
	}
	
	@Override
	public NounMetadata execute() {
		GenerateXRayHashingReactor hashReactor = new GenerateXRayHashingReactor();
		hashReactor.In();
		hashReactor.setNounStore(this.store);
		hashReactor.setInsight(this.insight);
		NounMetadata successfulHash = hashReactor.execute();
		if( ! ((boolean) successfulHash.getValue()) ) {
			throw new IllegalArgumentException("Error occured trying to generaate hash for xray");
		}
		
		// grab values from the hashReactor
		// since it already had to grab from store
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		String folderPath = hashReactor.getFolderPath();
		List<String> appIds = hashReactor.getAppIds();
		
		// get other parameters for xray script
		int nMinhash = 0;
		int nBands = 0;
		int instancesThreshold = 1;
		double similarityThreshold = getSimiliarityThreshold();
		double candidateThreshold = getCandidateThreshold();
		// check if user wants to compare columns from the same database
		// this is the boolean value passed into R script
		Boolean matchSameDB = true;
		if(this.keyValue.get("matchSameDb") != null) {
			matchSameDB = Boolean.parseBoolean(this.keyValue.get("matchSameDb"));
		}
		if (candidateThreshold <= 0.03) {
			nMinhash = 3640;
			nBands = 1820;
		} else if (candidateThreshold <= 0.02) {
			nMinhash = 8620;
			nBands = 4310;
		} else if (candidateThreshold <= 0.01) {
			nMinhash = 34480;
			nBands = 17240;
		} else if (candidateThreshold <= 0.05) {
			nMinhash = 1340;
			nBands = 670;
		} else if (candidateThreshold <= 0.1) {
			nMinhash = 400;
			nBands = 200;
		} else if (candidateThreshold <= 0.2) {
			nMinhash = 200;
			nBands = 100;
		} else if (candidateThreshold <= 0.4) {
			nMinhash = 210;
			nBands = 70;
		} else if (candidateThreshold <= 0.5) {
			nMinhash = 200;
			nBands = 50;
		} else {
			nMinhash = 200;
			nBands = 40;
		}
		
		// source the R script
		this.rJavaTranslator.executeEmptyR("source(\"" 
				+ DIHelper.getInstance().getProperty(Constants.BASE_FOLDER).replace('\\', '/') 
				+ "/R/XRay/matching.R\", local=TRUE);");

		logger.info("Running matching routine");
		String rFrameName = "xray_" + Utility.getRandomString(4);
		this.rJavaTranslator.executeEmptyR(rFrameName + " <- run_lsh_matching(\"" 
				+ folderPath + "\", " 
				+ nMinhash
				+ ", " + nBands 
				+ ", " + similarityThreshold 
				+ ", " + instancesThreshold 
				+ ", \";\", " 
				+ matchSameDB.toString().toUpperCase() + ");");
		logger.info("Done matching");

		this.rJavaTranslator.executeEmptyR(rFrameName + "<- as.data.table(" + rFrameName + ");");
		
		RDataTable matchingFrame = createNewFrameFromVariable(rFrameName);
		NounMetadata noun = new NounMetadata(matchingFrame, PixelDataType.FRAME, PixelOperationType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully ran LSH for matching column values"));
		
		// store the frame in the insight for use
		this.insight.getVarStore().put(rFrameName, noun);
		// set as default frame
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(matchingFrame);
		}
		
		return noun;
	}
	
	
	/**
	 * Get xray param to set the candidate threshold to match data
	 * @return candidateThreshold
	 */
	private double getCandidateThreshold() {
		double candidateThreshold = -1;
		String cand = this.keyValue.get("candidate");
		Double candidate = null;
		try {
			candidate = Double.parseDouble(cand);
		} catch(Exception e) {
			// ignore
		}
		if (candidate != null) {
			candidateThreshold = candidate.doubleValue();
		}
		// default value
		if (candidateThreshold < 0 || candidateThreshold > 1) {
			candidateThreshold = 0.01;
		}
		return candidateThreshold;
	}

	/**
	 * Get xray param to get similarity threshold
	 * @return
	 */
	private double getSimiliarityThreshold() {
		double similarityThreshold = -1;
		String sim = this.keyValue.get("similarity");
		Double similarity = null;
		try {
			similarity = Double.parseDouble(sim);
		} catch(Exception e) {
			// ignore
		}
		if (similarity != null) {
			similarityThreshold = similarity.doubleValue();
		}
		// default value
		if (similarityThreshold < 0 || similarityThreshold > 1) {
			similarityThreshold = 0.01;
		}
		return similarityThreshold;
	}
}