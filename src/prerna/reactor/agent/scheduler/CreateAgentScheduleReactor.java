package prerna.reactor.agent.scheduler;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Creates an owner-scoped Playground agent schedule. */
public class CreateAgentScheduleReactor extends AbstractReactor {

	private static final String NAME = "name";
	private static final String PROMPT = "prompt";
	private static final String AGENT_ID = "agentId";
	private static final String MODEL_ID = "modelId";
	private static final String SCHEDULE_TYPE = "scheduleType";
	private static final String CRON_EXPRESSION = "cronExpression";
	private static final String RUN_AT = "runAt";
	private static final String TIMEZONE = "timezone";
	private static final String ROOM_MODE = "roomMode";
	private static final String CONTINUING_ROOM_ID = "continuingRoomId";

	public CreateAgentScheduleReactor() {
		this.keysToGet = new String[] { NAME, PROMPT, AGENT_ID, MODEL_ID, SCHEDULE_TYPE, CRON_EXPRESSION, RUN_AT,
				TIMEZONE, ROOM_MODE, CONTINUING_ROOM_ID };
		this.keyRequired = new int[] { 0, 1, 0, 0, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = AgentScheduleService.create(this.insight, value(NAME), value(PROMPT),
				value(AGENT_ID), value(MODEL_ID), value(SCHEDULE_TYPE), value(CRON_EXPRESSION), value(RUN_AT),
				value(TIMEZONE), value(ROOM_MODE), value(CONTINUING_ROOM_ID));
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.SCHEDULE_JOB);
	}

	private String value(String key) {
		return this.keyValue.get(key);
	}

	@Override
	public String getReactorDescription() {
		return "Create a Playground RunAgent schedule owned by the authenticated user using the existing scheduler.";
	}
}
