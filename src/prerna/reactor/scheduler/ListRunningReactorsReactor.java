package prerna.reactor.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorExecutionTracker;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ListRunningReactorsReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ListRunningReactorsReactor.class);

	// Reactor execution tracker
	private static final ReactorExecutionTracker executionTracker = ReactorExecutionTracker.getInstance();

	public ListRunningReactorsReactor() {
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be admin to list running reactors");
		}

		classLogger.info("Listing all running reactors");

		Set<ReactorExecutionTracker.RunningReactorInfo> runningReactors = executionTracker.getAllRunningReactors();

		List<Map<String, Object>> reactorList = new ArrayList<>();

		for (ReactorExecutionTracker.RunningReactorInfo info : runningReactors) {
			Map<String, Object> reactorInfo = new HashMap<>();
			reactorInfo.put("reactorName", info.getReactorName());
			reactorInfo.put("threadId", info.getThreadId());
			reactorInfo.put("threadName", info.getThreadName());
			reactorInfo.put("jobId", info.getJobId());
			reactorInfo.put("sessionId", info.getSessionId());
			reactorInfo.put("startTime", info.getStartTime());
			reactorInfo.put("runningDurationMs", System.currentTimeMillis() - info.getStartTime());

			reactorList.add(reactorInfo);
		}

		Map<String, Object> result = new HashMap<>();
		result.put("count", reactorList.size());
		result.put("reactors", reactorList);
		result.put("timestamp", System.currentTimeMillis());

		classLogger.info("Found {} running reactors", reactorList.size());

		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.LIST_JOB);
	}

	@Override
	public String getReactorDescription() {
		return "Lists all currently running reactors with their execution context";
	}
}