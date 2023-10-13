package prerna.reactor.insights;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.project.api.IProject;
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
				ReactorKeysEnum.PROJECT.getKey(), 
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
		String projectId = getProject();
		if(projectId == null) {
			throw new IllegalArgumentException("Need to input the app name");
		}
		String rdbmsId = getRdbmsId();
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to input the id for the insight");
		}
		
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if(!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, rdbmsId)) {
			NounMetadata noun = new NounMetadata("User does not have access to this insight", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		}
		
		// get the engine so i can get the new insight
		IProject project = Utility.getProject(projectId);
		if(project == null) {
			throw new IllegalArgumentException("Cannot find project = " + projectId);
		}
		Insight newInsight = null;
		try {
			List<Insight> in = project.getInsight(rdbmsId + "");
			newInsight = in.get(0);
		} catch (ArrayIndexOutOfBoundsException e) {
			logger.info("Pulling app from cloud storage, projectId=" + projectId);
//			ClusterUtil.reactorPullInsightsDB(projectId);
			// this is needed for the pipeline json
			ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
			try {
				List<Insight> in = project.getInsight(rdbmsId + "");
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
		Map<String, Object> viewOptionsMap = PixelUtility.getInsightParameterJson(newInsight.getPixelList().getPixelRecipe());
		Map<String, Object> retMap = new HashMap<>();
		if(viewOptionsMap == null) {
			retMap.put("hasParameter", false);
		} else {
			retMap.put("hasParameter", true);
			retMap.put("viewOptionsMap", viewOptionsMap);
		}
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}