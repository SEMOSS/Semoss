package prerna.sablecc2.reactor.app;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
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

public class AddDefaultInsightsReactor extends AbstractReactor {

	private static final String CLASS_NAME = AddDefaultInsightsReactor.class.getName();
	
	private static final String INSIGHT_KEYS = "insights";
	
	private static final String EXPLORE_INSTANCE = "explore";
	private static final String GRID_DELTA_INSTANCE = "grid-delta";
	private static final String AUDIT_MODIFICATION = "audit-modification";
	private static final String AUDIT_TIMELINE = "audit-timeline";

	public AddDefaultInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), INSIGHT_KEYS};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
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
		
		logger.info("Retrieving app");
		IEngine engine = Utility.getEngine(appId);
		RDBMSNativeEngine insightEngine = engine.getInsightDatabase();
		ENGINE_TYPE eType = engine.getEngineType();
		if(addAll) {
			logger.info("Adding all default insights");
			String newInsightId = UploadUtilities.addExploreInstanceInsight(appId, insightEngine);
			addedInsight = true;
			registerInsightAndMetadata(appId, newInsightId, UploadUtilities.EXPLORE_INSIGHT_INSIGHT_NAME, UploadUtilities.EXPLORE_INSIGHT_LAYOUT);
			logger.info("Done adding explore an instance");
			if(eType == ENGINE_TYPE.RDBMS) {
				newInsightId = UploadUtilities.addGridDeltaInsight(appId, insightEngine);
				registerInsightAndMetadata(appId, newInsightId, UploadUtilities.GRID_DELTA_INSIGHT_NAME, UploadUtilities.GRID_DELTA_LAYOUT);
				logger.info("Done adding grid delta");
				
				// add audit insights
				// there could be an issue with loading the recipes to create
				newInsightId = UploadUtilities.addAuditModificationView(appId, insightEngine);
				if (newInsightId != null) {
					registerInsightAndMetadata(appId, newInsightId, UploadUtilities.AUDIT_MODIFICATION_VIEW_INSIGHT_NAME, UploadUtilities.AUDIT_MODIFICATION_VIEW_LAYOUT);
					logger.info("Done adding audit modification view");
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
				}
				
				newInsightId = UploadUtilities.addAuditTimelineView(appId, insightEngine);
				if (newInsightId != null) {
					registerInsightAndMetadata(appId, newInsightId, UploadUtilities.AUDIT_TIMELINE_INSIGHT_NAME, UploadUtilities.AUDIT_TIMELINE_LAYOUT);
					logger.info("Done adding audit timeline view");
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
				}
				
				
			}
		} else {
			if(insightsToAdd.contains(EXPLORE_INSTANCE)) {
				String newInsightId = UploadUtilities.addExploreInstanceInsight(appId, insightEngine);
				addedInsight = true;
				registerInsightAndMetadata(appId, newInsightId, UploadUtilities.EXPLORE_INSIGHT_INSIGHT_NAME, UploadUtilities.EXPLORE_INSIGHT_LAYOUT);
				logger.info("Done adding explore an instance");
			}
			if(insightsToAdd.contains(GRID_DELTA_INSTANCE)) {
				if(eType == ENGINE_TYPE.RDBMS) {
					String newInsightId = UploadUtilities.addGridDeltaInsight(appId, insightEngine);
					addedInsight = true;
					registerInsightAndMetadata(appId, newInsightId, UploadUtilities.GRID_DELTA_INSIGHT_NAME, UploadUtilities.GRID_DELTA_LAYOUT);
					logger.info("Done adding grid delta");
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This app is not an RDBMS so grid delta insight cannot be added"));
				}
			}
			if(insightsToAdd.contains(AUDIT_MODIFICATION)) {
				if(eType == ENGINE_TYPE.RDBMS) {
					String newInsightId = UploadUtilities.addAuditModificationView(appId, insightEngine);
					if (newInsightId != null) {
						addedInsight = true;
						registerInsightAndMetadata(appId, newInsightId, UploadUtilities.AUDIT_MODIFICATION_VIEW_INSIGHT_NAME, UploadUtilities.AUDIT_MODIFICATION_VIEW_LAYOUT);
						logger.info("Done adding audit modification view");
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This app is not an RDBMS so audit modification view insight cannot be added"));
				}
			}
			if(insightsToAdd.contains(AUDIT_TIMELINE)) {
				if(eType == ENGINE_TYPE.RDBMS) {
					String newInsightId = UploadUtilities.addAuditTimelineView(appId, insightEngine);
					if (newInsightId != null) {
						addedInsight = true;
						registerInsightAndMetadata(appId, newInsightId, UploadUtilities.AUDIT_TIMELINE_INSIGHT_NAME, UploadUtilities.AUDIT_TIMELINE_LAYOUT);
						logger.info("Done adding audit timeline view");
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This app is not an RDBMS so grid delta insight cannot be added"));
				}
			}
			if(insightsToAdd.contains("")) {
				
			}
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(addedInsight) {
			additionalNouns.add(NounMetadata.getSuccessNounMessage("Successfully added default insights"));
		}
		noun.addAllAdditionalReturn(additionalNouns);
		return noun;
	}
	
	private void registerInsightAndMetadata(String appId, String insightIdToSave, String insightName, String layout) {
		SecurityInsightUtils.addInsight(appId, insightIdToSave, insightName, true, layout);
		if(this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), appId, insightIdToSave);
		}
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
