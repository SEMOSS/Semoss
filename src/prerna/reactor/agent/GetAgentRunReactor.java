/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.agent;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRuntimeManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAgentRunReactor extends AbstractReactor {

	private static final String RUN_ID_KEY = "runId";

	public GetAgentRunReactor() {
		this.keysToGet = new String[] { RUN_ID_KEY };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String runId = StringUtils.trimToNull(this.keyValue.get(RUN_ID_KEY));
		Map<String, Object> result = AgentRuntimeManager.get().getRun(runId, this.insight);
		return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Get durable AgentRun status, output, message ids, error, and pending actions.";
	}
}
