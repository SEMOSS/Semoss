package prerna.reactor.agent.scheduler;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Internal fixed-recipe entry point invoked only by the scheduler. */
public class RunScheduledAgentReactor extends AbstractReactor {

	private static final String SCHEDULE_ID = "scheduleId";
	private static final String JOB_GROUP = "jobGroup";

	public RunScheduledAgentReactor() {
		this.keysToGet = new String[] { SCHEDULE_ID, JOB_GROUP };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		Map<String, Object> result = AgentScheduleService.runScheduled(this.insight, this.keyValue.get(SCHEDULE_ID),
				this.keyValue.get(JOB_GROUP));
		return new NounMetadata(result, PixelDataType.MAP);
	}

	@Override
	public String getReactorDescription() {
		return "Internal scheduler-only entry point for a fixed Playground agent schedule recipe.";
	}
}
