package prerna.usertracking.reactors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.usertracking.EngineUsageUtils;
import prerna.usertracking.EngineViewsUtils;
import prerna.usertracking.UserTrackingStatisticsUtils;
import prerna.util.Utility;

public class EngineActivityReactor extends AbstractReactor {
	
	private static final Logger logger = LogManager.getLogger(EngineActivityReactor.class);
	
	public EngineActivityReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		if (Utility.isUserTrackingDisabled()) {
			return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.USER_TRACKING_DISABLED);
		}
		
		String engineId = this.keyValue.get(this.keysToGet[0]);
		boolean addwarning = false;
		// TODO: account for legacy
		if(engineId == null) {
			engineId = this.keyValue.get(ReactorKeysEnum.DATABASE.getKey());
			if(engineId != null) {
				addwarning = true;
			}
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId) 
				&& !SecurityEngineUtils.engineIsDiscoverable(engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to engine");
		}
			
		logger.info("Getting engine activity for engine: {}", engineId);
		Map<String, Object> engineActivity = new HashMap<>();
		addTotalViews(engineActivity, engineId);
		addViewsByDate(engineActivity, engineId);
		addTotalUsesAndUsedIn(engineActivity, engineId);
		addUsesByDate(engineActivity, engineId);
		addUsabilityScore(engineActivity, engineId);
//		addDownloads(engineActivity, engineId);
		
//		return new NounMetadata(engineActivity, PixelDataType.MAP, PixelOperationType.ENGINE_ACTIVITY);
		NounMetadata noun = new NounMetadata(engineActivity, PixelDataType.MAP, PixelOperationType.ENGINE_ACTIVITY);
		if(addwarning) {
			noun.addAdditionalReturn(getWarning("Update reactor syntax to use engine= instead of database="));
		}
		return noun;
	}

	private void addTotalViews(Map<String, Object> engineActivity, String databaseId) {
		int totalViews = EngineViewsUtils.getTotal(databaseId);
		engineActivity.put("totalViews", totalViews);
	}

	private void addViewsByDate(Map<String, Object> engineActivity, String databaseId) {
		List<Pair<String, Integer>> viewsByDate = EngineViewsUtils.getByDate(databaseId);
		Map<String, Integer> vbd = viewsByDate.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
		engineActivity.put("viewsByDate", vbd);
	}

	private void addTotalUsesAndUsedIn(Map<String, Object> engineActivity, String databaseId) {
		List<Pair<String, String>> usesInInsights = EngineUsageUtils.getInInsights(databaseId);
		int totalUses = usesInInsights.size();
		engineActivity.put("totalUses", totalUses);
		
		User user = this.insight.getUser();
		List<Pair<String, String>> insightsUserCanView = usersCanView(usesInInsights, user);	
		List<Map<String, String>> usedIn = convertToMap(insightsUserCanView);
		engineActivity.put("usedIn", usedIn);
	}
	
	private List<Pair<String, String>> usersCanView(List<Pair<String, String>> usesInInsights, User user) {
		List<Pair<String, String>> insightsUserCanView = new ArrayList<>();
		insightsUserCanView = usesInInsights.stream()
				.filter(x -> SecurityInsightUtils.userCanViewInsight(user, x.getRight(), x.getLeft()))
				.collect(Collectors.toList());
		return insightsUserCanView;
	}
	
	private List<Map<String, String>> convertToMap(List<Pair<String, String>> insightsUserCanView) {
		return insightsUserCanView.stream().map(s -> {
			Map<String, String> m1 = new HashMap<>();
			m1.put("insightId", s.getLeft());
			m1.put("projectId", s.getRight());
			return m1;
		}).collect(Collectors.toList());
	}

	private void addUsesByDate(Map<String, Object> engineActivity, String databaseId) {
		List<Pair<String, Integer>> usesByDate = EngineUsageUtils.getByDate(databaseId);
		Map<String, Integer> ubd = usesByDate.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
		engineActivity.put("usesByDate", ubd);
	}
	
	private void addUsabilityScore(Map<String, Object> engineActivity, String databaseId) {
		engineActivity.put("usabilityScore", UserTrackingStatisticsUtils.calculateScore(databaseId));
	}
	
	// going to start tracking downloads later. adding it in now for front end
	private void addDownloads(Map<String, Object> engineActivity, String databaseId) {
		engineActivity.put("downloads", 0);
	}
}
