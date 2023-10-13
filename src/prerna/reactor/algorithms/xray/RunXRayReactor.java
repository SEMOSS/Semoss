package prerna.reactor.algorithms.xray;
//package prerna.sablecc2.reactor.algorithms.xray;
//
//import java.util.Map;
//
//import org.apache.logging.log4j.Logger;
//
//import prerna.ds.rdbms.h2.H2Frame;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.execptions.SemossPixelException;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
//import prerna.sablecc2.reactor.frame.r.GenerateH2FrameFromRVariableReactor;
//import prerna.util.usertracking.AnalyticsTrackerHelper;
//import prerna.util.usertracking.UserTrackerFactory;
//
///**
// * Writes instance data from CSV, EXCEL, LOCAL and EXTERNAL datasources to txt
// * files and compares them using R to find matching columns across
// * 
// * There are two modes to run R comparison
// * 
// * Data mode: compares encrypted instance data across datasources txt files are
// * written to SemossBase\R\XrayCompatibility\Temp\MatchingRepository
// * 
// * Semantic mode: predicts instance data headers based on wikipedia and compares
// * them across datasources files are written to
// * SemossBase\R\XrayCompatibility\Temp\SemanticRepository
// * 
// * RunXray("xrayConfigFile");
// */
//public class RunXRayReactor extends AbstractRFrameReactor {
//	private static final String CLASS_NAME = RunXRayReactor.class.getName();
//
//	public RunXRayReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.CONFIG.getKey() };
//	}
//
//	@Override
//	public NounMetadata execute() {
//		init();
//		Logger logger = getLogger(CLASS_NAME);
//		organizeKeys();
//		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
//		Map<String, Object> config = null;
//		if (grs != null && !grs.isEmpty()) {
//			config = (Map<String, Object>) grs.get(0);
//		} else {
//			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.CONFIG.getKey());
//		}
//
//		// output is written to this h2 frame
//		H2Frame frame = (H2Frame) getFrame();
//		Xray xray = new Xray(this.rJavaTranslator, getBaseFolder(), logger);
//		String rFrameName = xray.run(config);
//
//		// check if we have results from xray
//		String checkNull = "is.null(" + rFrameName + ")";
//		boolean nullResults = this.rJavaTranslator.getBoolean(checkNull);
//
//		// if we have results sync back SEMOSS
//		if (!nullResults) {
//			logger.info("Getting X-ray results into frame...");
//			GenerateH2FrameFromRVariableReactor sync = new GenerateH2FrameFromRVariableReactor();
//			sync.syncFromR(this.rJavaTranslator, rFrameName, frame);
//			NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.CODE_EXECUTION);
//
//			// clean up r temp variables
//			StringBuilder cleanUpScript = new StringBuilder();
//			cleanUpScript.append("rm(" + rFrameName + ");");
//			cleanUpScript.append("gc();");
//			this.rJavaTranslator.runR(cleanUpScript.toString());
//
//			// track GA data
////			UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "XRay");
//			
//			// NEW TRACKING
//			UserTrackerFactory.getInstance().trackAnalyticsWidget(
//					this.insight, 
//					frame, 
//					"XRay", 
//					AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
//			
//			return noun;
//		}
//		
//		// clean up r temp variables
//		StringBuilder cleanUpScript = new StringBuilder();
//		cleanUpScript.append("rm(" + rFrameName + ");");
//		cleanUpScript.append("gc();");
//		this.rJavaTranslator.runR(cleanUpScript.toString());
//		
//		NounMetadata noun = new NounMetadata("Unable to obtain X-ray results", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
//		SemossPixelException exception = new SemossPixelException(noun);
//		exception.setContinueThreadOfExecution(false);
//		throw exception;
//	}
//
//}
