package prerna.reactor.agent.scheduler;

import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** Lists only the authenticated user's Playground agent schedules. */
public class ListAgentSchedulesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<Map<String, Object>> result = AgentScheduleService.list(this.insight);
		return new NounMetadata(result, PixelDataType.VECTOR);
	}

	@Override
	public String getReactorDescription() {
		return "List the authenticated user's Playground agent schedules and their latest run state.";
	}
}
