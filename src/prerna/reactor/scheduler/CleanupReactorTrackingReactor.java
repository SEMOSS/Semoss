package prerna.reactor.scheduler;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorExecutionTracker;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CleanupReactorTrackingReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CleanupReactorTrackingReactor.class);

	// Reactor execution tracker
	private static final ReactorExecutionTracker executionTracker = ReactorExecutionTracker.getInstance();

	public CleanupReactorTrackingReactor() {
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User must be admin to cleanup reactor tracking");
		}

		classLogger.info("Cleaning up reactor tracking data");

		// Get counts before cleanup
		int beforeThreadCount = executionTracker.getAllRunningReactors().size();

		// Perform cleanup
		executionTracker.clearAllTracking();

		Map<String, Object> result = new HashMap<>();
		result.put("status", "cleanup-completed");
		result.put("message", "Reactor tracking cleanup completed");
		result.put("reactorsCleared", beforeThreadCount);
		result.put("timestamp", System.currentTimeMillis());

		classLogger.info("Reactor tracking cleanup completed. {} reactor entries cleared", beforeThreadCount);

		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Cleans up stale reactor tracking data";
	}
}