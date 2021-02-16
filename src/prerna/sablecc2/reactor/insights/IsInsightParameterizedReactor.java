package prerna.sablecc2.reactor.insights;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class IsInsightParameterizedReactor extends AbstractInsightReactor {
	
	private static final String CLASS_NAME = IsInsightParameterizedReactor.class.getName();
	
	public IsInsightParameterizedReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		/*
		 * 1) Start Permission checks / pulling the recipe from the insights database
		 */
		
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String appId = getApp();
		if(appId == null) {
			throw new IllegalArgumentException("Need to input the app name");
		}
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to input the id for the insight");
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), appId, rdbmsId)) {
				NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
		}
		
		// get the engine so i can get the new insight
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find app = " + appId);
		}
		Insight newInsight = null;
		try {
			List<Insight> in = engine.getInsight(rdbmsId + "");
			newInsight = in.get(0);
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.info("Pulling app from cloud storage, appid=" + appId);
			ClusterUtil.reactorPullInsightsDB(appId);
			// this is needed for the pipeline json
			ClusterUtil.reactorPullFolder(engine, AssetUtility.getAppAssetVersionFolder(engine.getEngineName(), appId));
			try {
				List<Insight> in = engine.getInsight(rdbmsId + "");
				newInsight = in.get(0);
			} catch(IllegalArgumentException e2) {
				NounMetadata noun = new NounMetadata(e2.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			} catch (ArrayIndexOutOfBoundsException e2) {
				NounMetadata noun = new NounMetadata("Insight does not exist", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
		}
		
		/*
		 * 2) Legacy insight check - not really important for most developers
		 */
		
		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			// cannot do parameters on legacy insights
			return new NounMetadata(false, PixelDataType.BOOLEAN);
		}
		
		// yay... not legacy
		
		// parse the recipe and return if it is a param
		boolean isParam = PixelUtility.hasParam(newInsight.getPixelList().getPixelRecipe());
		return new NounMetadata(isParam, PixelDataType.BOOLEAN);
	}

}