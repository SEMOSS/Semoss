package prerna.reactor.agent.scheduler;

import java.util.LinkedHashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Updates, pauses, resumes, deletes, or immediately triggers an owned schedule. */
public class ManageAgentScheduleReactor extends AbstractReactor {

	private static final String SCHEDULE_ID = "scheduleId";
	private static final String ACTION = "action";
	private static final String[] UPDATE_KEYS = { "name", "prompt", "agentId", "modelId", "scheduleType",
			"cronExpression", "runAt", "timezone", "roomMode", "continuingRoomId" };

	public ManageAgentScheduleReactor() {
		this.keysToGet = new String[UPDATE_KEYS.length + 2];
		this.keysToGet[0] = SCHEDULE_ID;
		this.keysToGet[1] = ACTION;
		System.arraycopy(UPDATE_KEYS, 0, this.keysToGet, 2, UPDATE_KEYS.length);
		this.keyRequired = new int[this.keysToGet.length];
		this.keyRequired[0] = 1;
		this.keyRequired[1] = 1;
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, String> updates = new LinkedHashMap<>();
		for (String key : UPDATE_KEYS) {
			String value = this.keyValue.get(key);
			if (value != null) {
				updates.put(key, value);
			}
		}
		Map<String, Object> result = AgentScheduleService.manage(this.insight, this.keyValue.get(SCHEDULE_ID),
				this.keyValue.get(ACTION), updates);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
	}

	@Override
	public String getReactorDescription() {
		return "Manage an owned Playground agent schedule. Supported actions are UPDATE, PAUSE, RESUME, DELETE, and RUN_NOW.";
	}
}
