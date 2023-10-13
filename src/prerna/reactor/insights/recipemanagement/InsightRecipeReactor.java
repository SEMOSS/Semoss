package prerna.reactor.insights.recipemanagement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.project.api.IProject;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class InsightRecipeReactor extends AbstractInsightReactor {
	
	private static final String VECTOR_KEY = "vec";
	
	public InsightRecipeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), VECTOR_KEY};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		boolean vec = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));
		// get the engine so i can get the new insight
		IProject project = Utility.getProject(projectId);
		if(project == null) {
			throw new IllegalArgumentException("Cannot find project = " + projectId);
		}
		List<Insight> in = project.getInsight(rdbmsId);
		Insight newInsight = in.get(0);
		
		// OLD INSIGHT
		if(newInsight instanceof OldInsight) {
			Map<String, Object> insightMap = new HashMap<String, Object>();
			// return to the FE the recipe
			insightMap.put("name", newInsight.getInsightName());
			// keys below match those in solr
			insightMap.put("core_engine", newInsight.getProjectId());
			insightMap.put("core_engine_id", newInsight.getRdbmsId());
			return new NounMetadata(insightMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OLD_INSIGHT);
		}

		List<String> recipeSteps = newInsight.getPixelList().getPixelRecipe();
		if(vec) {
			return new NounMetadata(recipeSteps, PixelDataType.CONST_STRING, PixelOperationType.SAVED_INSIGHT_RECIPE);
		}
		
		// combine into single string
		StringBuilder bigRecipe = new StringBuilder();
		int size = recipeSteps.size();
		int i = 0;
		for(; i < size; i++) {
			bigRecipe.append(recipeSteps.get(i));
		}
		
		// return the recipe steps
		return new NounMetadata(bigRecipe.toString(), PixelDataType.CONST_STRING, PixelOperationType.SAVED_INSIGHT_RECIPE);
	}
}