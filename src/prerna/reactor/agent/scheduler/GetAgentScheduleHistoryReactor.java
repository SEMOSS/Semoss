package prerna.reactor.agent.scheduler;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Returns existing scheduler audit rows for one owned agent schedule. */
public class GetAgentScheduleHistoryReactor extends AbstractReactor {

	private static final String SCHEDULE_ID = "scheduleId";
	private static final String LIMIT = "limit";

	public GetAgentScheduleHistoryReactor() {
		this.keysToGet = new String[] { SCHEDULE_ID, LIMIT };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		int limit = 50;
		String rawLimit = this.keyValue.get(LIMIT);
		if (rawLimit != null && !rawLimit.isBlank()) {
			try {
				limit = Integer.parseInt(rawLimit);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("limit must be an integer", e);
			}
		}
		List<Map<String, Object>> result = AgentScheduleService.history(this.insight,
				this.keyValue.get(SCHEDULE_ID), limit);
		return new NounMetadata(result, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
		return "Get scheduler audit history for one Playground agent schedule owned by the authenticated user.";
	}
}
