package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;
import prerna.auth.User;

public class GetUserConversationRoomsReactor extends AbstractReactor {
	@SuppressWarnings("unused")
	private static final Logger logger = LogManager.getLogger(GetUserConversationRoomsReactor.class);

    public GetUserConversationRoomsReactor() {
        this.keysToGet = new String[] {
                ReactorKeysEnum.PROJECT.getKey(),
                ReactorKeysEnum.LIMIT.getKey(),   
                ReactorKeysEnum.OFFSET.getKey(),  
                ReactorKeysEnum.SEARCH.getKey(), 
                ReactorKeysEnum.SORT.getKey() 
            };
        this.keyRequired = new int[] {0,0,0,0,0};
    }
    
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }
        String projectId = this.keyValue.get(this.keysToGet[0]);
        if (projectId == null) {
        	projectId = this.insight.getContextProjectId();
        } 
        
        long limit = getLimit();
        long offset = getOffset();

        // Only accept "asc" or "desc", default to DESC
        String sortDir = this.keyValue.getOrDefault("sort", "DESC");
        sortDir = (sortDir != null) ? sortDir.trim().toUpperCase() : "DESC";
        if (!sortDir.equals("ASC") && !sortDir.equals("DESC")) sortDir = "DESC";
        
		String search = this.keyValue.get(ReactorKeysEnum.SEARCH.getKey());
		if (search != null) {
			search = Utility.decodeURIComponent(search);
		}
		     
        // Call new overload of getUserConversations
        List<Map<String, Object>> output = ModelInferenceLogsUtils.getUserConversations(
            user.getPrimaryLoginToken().getId(),
            projectId,
            limit,
            offset,
            sortDir,
            search
        );
        
        // Register insights for each room_id returned
        if (output != null && !output.isEmpty()) {
            for (Map<String, Object> convo : output) {
                createInsights((String) convo.get("ROOM_ID"));
            }
        }
        
        return new NounMetadata(output, PixelDataType.VECTOR);
	}
	
	
	public NounMetadata createInsights(String insightId) {
		Insight newInsight = new Insight();
		if (insightId != null && !insightId.isEmpty() && !InsightStore.getInstance().containsKey(insightId)) {
			List<Map<String, Object>> output = ModelInferenceLogsUtils.doVerifyConversation(this.insight.getUserId(), insightId);
			if (output.size() > 0) {
				newInsight.setInsightId(insightId);
				String projectId = (String) output.get(0).get("PROJECT_ID");
				if (projectId != null && !projectId.isEmpty()) {
					newInsight.setProjectId(projectId);
				}
			} else {
				if (this.insight.getProjectId() != null && !this.insight.getProjectId().isEmpty()) {
					newInsight.setProjectId(this.insight.getProjectId());
				} 
			}
		}
		
		if (this.insight.getContextProjectId() != null && !this.insight.getContextProjectId().isEmpty()) {
			newInsight.setContextProjectId(this.insight.getContextProjectId());
		} 
		
		newInsight.setCacheInWorkspace(true);
		InsightUtility.transferDefaultVars(this.insight, newInsight);
		InsightStore.getInstance().put(newInsight);
		InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
		// set the user in the insight
		newInsight.setUser(this.insight.getUser());

		List<String> newRecipe = new Vector<String>();

		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", newInsight.runPixel(newRecipe));
		return new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.NEW_EMPTY_INSIGHT);
	}
	


	  private long getLimit() {
	    GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.LIMIT.getKey());
	    if (inputsGRS != null && !inputsGRS.isEmpty()) {
	      NounMetadata limitNoun = inputsGRS.getNoun(0);
	      return ((Number) limitNoun.getValue()).longValue();
	    }
	    return -1;
	  }

	  private long getOffset() {
	    GenRowStruct inputsGRS = this.store.getGenRowStruct(ReactorKeysEnum.OFFSET.getKey());
	    if (inputsGRS != null && !inputsGRS.isEmpty()) {
	      NounMetadata offsetNoun = inputsGRS.getNoun(0);
	      return ((Number) offsetNoun.getValue()).longValue();
	    }
	    return -1;
	  }
}
