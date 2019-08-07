package prerna.sablecc2.reactor.app;

import java.util.List;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.util.Utility;

public class ResetDefaultInsightsReactor extends AbstractReactor {

	private static final String INSIGHT_KEYS = "insights";
	
	private static final String EXPLORE_INSTANCE = "explore";
	private static final String GRID_DELTA_INSTANCE = "grid-delta";
	
	public ResetDefaultInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), INSIGHT_KEYS};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		
		// security and stuff
		if(AbstractSecurityUtils.securityEnabled()) {
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityAppUtils.userCanEditEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException("User does not have permission to add insights in the app");
			}
		}

		List<String> insightsToAdd = getDefaultInsights();
		boolean addAll = false;
		if(insightsToAdd.isEmpty()) {
			addAll = true;
		}
		
		List<NounMetadata> additionalNouns = new Vector<NounMetadata>();
		boolean addedInsight = false;
		// already have default methods to add
		IEngine engine = Utility.getEngine(appId);
		RDBMSNativeEngine insightEngine = engine.getInsightDatabase();
		ENGINE_TYPE eType = engine.getEngineType();
		if(addAll) {
			UploadUtilities.addExploreInstanceInsight(appId, insightEngine);
			addedInsight = true;
			if(eType == ENGINE_TYPE.RDBMS) {
				UploadUtilities.addGridDeltaInsight(appId, insightEngine);
			}
		} else {
			if(insightsToAdd.contains(EXPLORE_INSTANCE)) {
				UploadUtilities.addExploreInstanceInsight(appId, insightEngine);
				addedInsight = true;
			}
			if(insightsToAdd.contains(GRID_DELTA_INSTANCE)) {
				if(eType == ENGINE_TYPE.RDBMS) {
					UploadUtilities.addGridDeltaInsight(appId, insightEngine);
					addedInsight = true;
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This app is not an RDBMS so grid delta insight cannot be added"));
				}
			}
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(addedInsight) {
			additionalNouns.add(NounMetadata.getSuccessNounMessage("Successfully added default insights"));
		}
		noun.addAllAdditionalReturn(additionalNouns);
		return noun;
	}
	
	private List<String> getDefaultInsights() {
		GenRowStruct grs = this.store.getNoun(INSIGHT_KEYS);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		List<String> curStringValues = this.curRow.getAllStrValues();
		// see if appId was defined via key or also inline
		if(this.store.getNoun(this.keysToGet[0]) == null || this.store.getNoun(this.keysToGet[0]).isEmpty()) {
			// app id was inline
			// remove index 0
			curStringValues.remove(0);
		}
		
		return curStringValues;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(INSIGHT_KEYS)) {
			return "Determine which default insights to append. "
					+ "Value '" + EXPLORE_INSTANCE + "' = 'Explore an Instance', "
					+ "value '" + GRID_DELTA_INSTANCE + "' = 'Grid Delta'."
					+ "Grid Delta only adds if database is type RDBMS."
					+ "No inputs passed will attempt to add all default insight.";
		}
		return super.getDescriptionForKey(key);
	}

}
