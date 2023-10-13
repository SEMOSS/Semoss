package prerna.reactor.algorithms.xray;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenerateXRayMatchingReactor extends AbstractRFrameReactor {

	public static final String CLASS_NAME = GenerateXRayMatchingReactor.class.getName();
	
	public static final String SIMILARITY_KEY = "similarity";
	public static final String CANDIDATE_KEY = "candidate";
	public static final String MATCH_SAME_DB_KEY = "matchSameDb";
	public static final String ROW_MATCHING = "rowComparison";

	public GenerateXRayMatchingReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), 
				ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.OVERRIDE.getKey(), ReactorKeysEnum.CONFIG.getKey(),
				SIMILARITY_KEY, CANDIDATE_KEY, MATCH_SAME_DB_KEY, ROW_MATCHING};
	}
	
	@Override
	public NounMetadata execute() {
		GenerateXRayHashingReactor hashReactor = new GenerateXRayHashingReactor();
		hashReactor.In();
		hashReactor.setNounStore(this.store);
		hashReactor.setInsight(this.insight);
		NounMetadata successfulHash = hashReactor.execute();
		
		Map<String, Object> filesHash = null;
		try {
			filesHash = (Map<String, Object>) successfulHash.getValue();
		} catch(Exception e) {
			throw new IllegalArgumentException("Error occurred trying to generaate hash for xray");
		}
		// specify the specific files to use
		List<String> fileNames = (List<String>) filesHash.get(GenerateXRayHashingReactor.FILES_KEY);
		if(fileNames == null || fileNames.isEmpty()) {
			throw new IllegalArgumentException("Error occurred trying to generaate hash for xray");
		}
		List<String> databaseIds = (List<String>) filesHash.get(GenerateXRayHashingReactor.DATABASE_IDS_KEY);
		
		// grab values from the hashReactor
		// since it already had to grab from store
		init();
		Logger logger = this.getLogger(CLASS_NAME);
		this.keyValue = hashReactor.keyValue;
		// get the exact files that were generated
		String folderPath = hashReactor.getFolderPath();
		List<String> filePaths = new Vector<String>(fileNames.size());
		for(int i = 0; i < fileNames.size(); i++) {
			filePaths.add(folderPath + "/" + fileNames.get(i));
		}
		
		// get other parameters for xray script
		int nMinhash = 0;
		int nBands = 0;
		int instancesThreshold = 1;
		double similarityThreshold = getSimiliarityThreshold();
		double candidateThreshold = getCandidateThreshold();
		// check if user wants to compare columns from the same database
		// this is the boolean value passed into R script
		Boolean matchSameDB = true;
		if(this.keyValue.get(MATCH_SAME_DB_KEY) != null) {
			matchSameDB = Boolean.parseBoolean(this.keyValue.get(MATCH_SAME_DB_KEY));
		}
		boolean addSameDbWarn = false;
		if(databaseIds.size() == 1) {
			if(!matchSameDB) {
				addSameDbWarn = true;
			}
			matchSameDB = true;
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
		String script = rFrameName + " <- run_lsh_matching(" 
				+ RSyntaxHelper.createStringRColVec(filePaths) + ", " 
				+ nMinhash
				+ ", " + nBands 
				+ ", " + similarityThreshold 
				+ ", " + instancesThreshold 
				+ ", \";\", " 
				+ matchSameDB.toString().toUpperCase() + ");";
		this.rJavaTranslator.executeEmptyR(script);
		logger.info("Done matching");
		
		this.rJavaTranslator.executeEmptyR(rFrameName + "<- as.data.table(" + rFrameName + ");");
		
		// see if we can replace database ids with database name
		boolean replaceIds = true;
		List<String> databaseNames = new Vector<String>(databaseIds.size());
		for(int i = 0; i < databaseIds.size(); i++) {
			String databaseName = MasterDatabaseUtility.getDatabaseAliasForId(databaseIds.get(i));
			if(databaseNames.contains(databaseName)) {
				replaceIds = false;
				break;
			}
			databaseNames.add(databaseName);
		}
		if(replaceIds) {
			StringBuilder replaceSyntax = new StringBuilder();
			String sourceDbId = rFrameName + "$Source_Database_Id";
			String targetDbId = rFrameName + "$Target_Database_Id";
			for(int i = 0; i < databaseIds.size(); i++) {
				String databaseId = databaseIds.get(i);
				String databaseName = databaseNames.get(i);
				
				replaceSyntax.append(sourceDbId + "[" + sourceDbId + " == \"" + databaseId + "\"] <- \"" + databaseName + "\";");
				replaceSyntax.append(targetDbId + "[" + targetDbId + " == \"" + databaseId + "\"] <- \"" + databaseName + "\";");
			}
			this.rJavaTranslator.executeEmptyR(replaceSyntax.toString());
		}
		
		RDataTable matchingFrame = createNewFrameFromVariable(rFrameName);
		NounMetadata noun = new NounMetadata(matchingFrame, PixelDataType.FRAME, PixelOperationType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully ran LSH for matching column values"));
		if(addSameDbWarn) {
			noun.addAdditionalReturn(NounMetadata.getWarningNounMessage("Since only one database was selected, altered input value to perform same database matching"));
		}
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
		String cand = this.keyValue.get(CANDIDATE_KEY);
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
		String sim = this.keyValue.get(SIMILARITY_KEY);
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