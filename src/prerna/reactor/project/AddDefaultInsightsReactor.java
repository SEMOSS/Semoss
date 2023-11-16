package prerna.reactor.project;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.upload.UploadUtilities;

public class AddDefaultInsightsReactor extends AbstractReactor {

	private static final String CLASS_NAME = AddDefaultInsightsReactor.class.getName();
	
	private static final String INSIGHT_KEYS = "insights";
	
	private static final String EXPLORE_INSTANCE = "explore";
	private static final String GRID_DELTA_INSTANCE = "grid-delta";
	private static final String AUDIT_MODIFICATION = "audit-modification";
	private static final String AUDIT_TIMELINE = "audit-timeline";
	private static final String INSIGHT_STATS = "insight-stats";
	private static final String INSERT_FORM = "insert-form";
	private static final String UPDATE_FORM = "update-form";

	public AddDefaultInsightsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.DATABASE.getKey(), INSIGHT_KEYS};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String databaseId = this.keyValue.get(this.keysToGet[1]);
		List<String> insightsToAdd = getDefaultInsights();
		boolean addAll = false;
		if(insightsToAdd.isEmpty()) {
			addAll = true;
		}
		boolean pullDatabase = true;
		// security and stuff
		if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
			throwAnonymousUserError();
		}
		
		if(!SecurityProjectUtils.userCanEditProject(this.insight.getUser(), projectId)) {
			throw new IllegalArgumentException("User does not have permission to add insights in the project");
		}
		if(!addAll && insightsToAdd.size()==1 && insightsToAdd.contains(INSIGHT_STATS)) {
			// do not need a database for this situation
			pullDatabase = false;
		} else if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("User does not have permission to view the database");
		}

		List<NounMetadata> additionalNouns = new Vector<NounMetadata>();
		boolean addedInsight = false;
		// already have default methods to add
		
		logger.info("Retrieving database");
		IProject project = Utility.getProject(projectId);
		RDBMSNativeEngine insightEngine = project.getInsightDatabase();
		String projectName = project.getProjectName();
		
		String databaseName = "";
		IDatabaseEngine.DATABASE_TYPE dbType = null;
		if(pullDatabase) {
			IDatabaseEngine database = Utility.getDatabase(databaseId);
			databaseName = database.getEngineName();
			dbType = database.getDatabaseType();
		}
		
		boolean cacheable = Utility.getApplicationCacheInsight();
		int cacheMinutes = Utility.getApplicationCacheInsightMinutes();
		boolean cacheEncrypt = Utility.getApplicationCacheEncrypt();
		String cacheCron = Utility.getApplicationCacheCron();
		ZonedDateTime cachedOn = null;
		
//		ClusterUtil.reactorPullInsightsDB(projectId);
		if(addAll) {
			addedInsight = true;

			logger.info("Adding all default insights");
			Map<String, Object> retMap = UploadUtilities.addExploreInstanceInsight(projectId, projectName, databaseId, databaseName, insightEngine);
			String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
			List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
			String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
			String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
			registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.EXPLORE_INSIGHT_LAYOUT, 
					cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
			logger.info("Done adding explore an instance");
			
			retMap = UploadUtilities.addInsightUsageStats(projectId, projectName, insightEngine);
			newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
			recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
			insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
			schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
			registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.INSIGHT_USAGE_STATS_LAYOUT, 
					cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
			logger.info("Done adding insight usage stats");
			
			if(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
				retMap = UploadUtilities.addGridDeltaInsight(projectId, projectName, databaseId, databaseName, insightEngine);
				newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
				recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
				insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
				schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
				registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.GRID_DELTA_LAYOUT, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
				logger.info("Done adding grid delta");
				
				// add audit insights
				// there could be an issue with loading the recipes to create
				retMap = UploadUtilities.addAuditModificationView(projectId, projectName, databaseId, databaseName, insightEngine);
				if(retMap != null) {
					newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
					recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
					insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
					schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
					if (newInsightId != null) {
						registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.AUDIT_MODIFICATION_VIEW_LAYOUT, 
								cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
						logger.info("Done adding audit modification view");
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
				}
				
				retMap = UploadUtilities.addAuditTimelineView(projectId, projectName, databaseId, databaseName, insightEngine);
				if(retMap != null) {
					newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
					recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
					insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
					schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
					if (newInsightId != null) {
						registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.AUDIT_TIMELINE_LAYOUT, 
								cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
						logger.info("Done adding audit timeline view");
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
				}
				
			}
		} else {
			if(insightsToAdd.contains(EXPLORE_INSTANCE)) {
				Map<String, Object> retMap = UploadUtilities.addExploreInstanceInsight(projectId, projectName, databaseId, databaseName, insightEngine);
				String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
				List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
				String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
				String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
				addedInsight = true;
				registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.EXPLORE_INSIGHT_LAYOUT, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
				logger.info("Done adding explore an instance");
			}
			if(insightsToAdd.contains(INSIGHT_STATS)) {
				Map<String, Object> retMap = UploadUtilities.addInsightUsageStats(projectId, projectName, insightEngine);
				String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
				List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
				String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
				String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
				addedInsight = true;
				registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.INSIGHT_USAGE_STATS_LAYOUT, 
						cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
				logger.info("Done adding insight usage stats");
			}
			if(insightsToAdd.contains(GRID_DELTA_INSTANCE)) {
				if(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
					Map<String, Object> retMap = UploadUtilities.addGridDeltaInsight(projectId, projectName, databaseId, databaseName, insightEngine);
					String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
					List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
					String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
					String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
					addedInsight = true;
					registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.GRID_DELTA_LAYOUT,
							cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
					logger.info("Done adding grid delta");
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This database is not an RDBMS so grid delta insight cannot be added"));
				}
			}
			if(insightsToAdd.contains(AUDIT_MODIFICATION)) {
				if(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
					Map<String, Object> retMap = UploadUtilities.addAuditModificationView(projectId, projectName, databaseId, databaseName, insightEngine);
					if(retMap != null) {
						String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
						List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
						String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
						String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
						if (newInsightId != null) {
							addedInsight = true;
							registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.AUDIT_MODIFICATION_VIEW_LAYOUT, 
									cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
							logger.info("Done adding audit modification view");
						} else {
							additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
						}
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This database is not an RDBMS so audit modification view insight cannot be added"));
				}
			}
			if(insightsToAdd.contains(AUDIT_TIMELINE)) {
				if(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
					Map<String, Object> retMap = UploadUtilities.addAuditTimelineView(projectId, projectName, databaseId, databaseName, insightEngine);
					if(retMap != null) {
						String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
						List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
						String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
						String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
						if (newInsightId != null) {
							addedInsight = true;
							registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.AUDIT_TIMELINE_LAYOUT, 
									cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
							logger.info("Done adding audit timeline view");
						} else {
							additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit timeline view"));
						}
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add audit modification view"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This database is not an RDBMS so grid delta insight cannot be added"));
				}
			}
			if(insightsToAdd.contains(INSERT_FORM)) {
				if(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS) {
					Map<String, Object> retMap = UploadUtilities.addInsertFormInsight(projectId, projectName, databaseId, databaseName, insightEngine);
					if(retMap != null) {
						String newInsightId = (String) retMap.get(UploadUtilities.INSIGHT_ID_KEY);
						List<String> recipe = (List<String>) retMap.get(UploadUtilities.RECIPE_ID_KEY);
						String insightName = (String) retMap.get(UploadUtilities.INSIGHT_NAME_KEY);
						String schemaName = (String) retMap.get(UploadUtilities.SCHEMA_NAME_KEY);
						if (newInsightId != null) {
							addedInsight = true;
							registerInsightAndMetadata(projectId, newInsightId, insightName, UploadUtilities.INSERT_FORM_LAYOUT, 
									cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
							logger.info("Done adding insert form insight");
						} else {
							additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add insert form insight"));
						}
					} else {
						additionalNouns.add(NounMetadata.getWarningNounMessage("Unable to add insert form insight"));
					}
				} else {
					additionalNouns.add(NounMetadata.getWarningNounMessage("This database is not an RDBMS so grid delta insight cannot be added"));
				}
			}
		}
		
		// push to the cloud
//		ClusterUtil.reactorPushInsightDB(projectId);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN); 
		if(addedInsight) {
			additionalNouns.add(NounMetadata.getSuccessNounMessage("Successfully added default insights"));
		}
		noun.addAllAdditionalReturn(additionalNouns);
		return noun;
	}
	
	private void registerInsightAndMetadata(String projectId, String insightIdToSave, String insightName, String layout, 
			boolean cacheable, int cacheMinutes, String cacheCron, ZonedDateTime cachedOn, boolean cacheEncrypt, 
			List<String> recipe, String schemaName) {
		SecurityInsightUtils.addInsight(projectId, insightIdToSave, insightName, true, layout, 
				cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, recipe, schemaName);
		if(this.insight.getUser() != null) {
			SecurityInsightUtils.addUserInsightCreator(this.insight.getUser(), projectId, insightIdToSave);
		}
	}
	
	private List<String> getDefaultInsights() {
		GenRowStruct grs = this.store.getNoun(INSIGHT_KEYS);
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		List<String> curStringValues = this.curRow.getAllStrValues();
		// see if databaseId was defined via key or also inline
		if(this.store.getNoun(this.keysToGet[0]) == null || this.store.getNoun(this.keysToGet[0]).isEmpty()) {
			// database id was inline
			// remove index 0
			curStringValues.remove(0);
		}
		
		return curStringValues;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(INSIGHT_KEYS)) {
			return "Determine which default insights to append. "
					+ "Value '" + EXPLORE_INSTANCE + "' = '" + UploadUtilities.EXPLORE_INSIGHT_INSIGHT_NAME + "', "
					+ "Value '" + AUDIT_MODIFICATION + "' = '" + UploadUtilities.AUDIT_MODIFICATION_VIEW_INSIGHT_NAME + "', "
					+ "Value '" + AUDIT_TIMELINE + "' = '" + UploadUtilities.AUDIT_TIMELINE_INSIGHT_NAME + "', "
					+ "value '" + GRID_DELTA_INSTANCE + "' = '" + UploadUtilities.GRID_DELTA_INSIGHT_NAME + "'."
					+ UploadUtilities.GRID_DELTA_INSIGHT_NAME + " only adds if database is type RDBMS."
					+ "No inputs passed will attempt to add all default insight.";
		}
		return super.getDescriptionForKey(key);
	}

}
