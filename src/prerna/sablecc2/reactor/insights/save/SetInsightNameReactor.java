package prerna.sablecc2.reactor.insights.save;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.InsightAdministrator;
import prerna.om.MosfetFile;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;
import prerna.util.AssetUtility;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class SetInsightNameReactor extends AbstractInsightReactor {

	private static final String CLASS_NAME = SetInsightNameReactor.class.getName();

	public SetInsightNameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey(), ReactorKeysEnum.ID.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		String projectId = getProject();
		// need to know what we are updating
		String existingId = getRdbmsId();
		
		// we may have the alias
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, existingId)) {
			throw new IllegalArgumentException("Project does not exist or user does not have permission to edit this insight");
		}
		
		String insightName = getInsightName();
		if(insightName == null || (insightName = insightName.trim()).isEmpty()) {
			throw new IllegalArgumentException("Need to define the insight name");
		}
		
		if(SecurityInsightUtils.insightNameExistsMinusId(projectId, insightName, existingId)) {
			throw new IllegalArgumentException("Insight name already exists");
		}
		
		IProject project = Utility.getProject(projectId);
		// add the recipe to the insights database
		InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());

		// update insight db
		logger.info("1) Updating insight in rdbms");
		admin.updateInsightName(existingId, insightName);
		logger.info("1) Done");
		
		logger.info("2) Updating insight in index");
		SecurityInsightUtils.updateInsightName(projectId, existingId, insightName);
		logger.info("2) Done");
		
		logger.info("3) Update mosfet file for collaboration");
		updateRecipeFile(logger, project.getProjectId(), project.getProjectName(), existingId, insightName);
		logger.info("3) Done");

//		ClusterUtil.reactorPushInsightDB(projectId);
		
		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("app_insight_id", existingId);
		returnMap.put("app_name", project.getProjectName());
		returnMap.put("app_id", project.getProjectId());
		
		returnMap.put("name", insightName);
		returnMap.put("project_insight_id", existingId);
		returnMap.put("project_name", project.getProjectName());
		returnMap.put("project_id", project.getProjectId());
		NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.SAVE_INSIGHT);
		return noun;
	}

	/**
	 * Update recipe: delete the old file and save as new
	 * 
	 * @param engineName
	 * @param rdbmsID
	 * @param recipeToSave
	 */
	protected void updateRecipeFile(Logger logger, String projectId, String projectName, String rdbmsID, String insightName) {
		String recipeLocation = AssetUtility.getProjectVersionFolder(projectName, projectId)
				+ DIR_SEPARATOR + rdbmsID + DIR_SEPARATOR + MosfetFile.RECIPE_FILE;
		File mosfet = new File(recipeLocation);
		if(mosfet.exists()) {
			try {
				MosfetSyncHelper.updateMosfitFileInsightName(new File(recipeLocation), insightName);
				// add to git
				String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
				List<String> files = new Vector<>();
				files.add(rdbmsID + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);		
				GitRepoUtils.addSpecificFiles(gitFolder, files);
				GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Changed " + insightName + " insight name"));
			} catch (Exception e) {
				logger.info("Error occurred trying to write to git folder");
				e.printStackTrace();
			}
		} else {
			logger.info("... Could not find existing mosfet file. Ignoring update.");
		}
	}
	
}
