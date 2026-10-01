/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;

import com.google.gson.Gson;

import prerna.om.ThreadStore;
import prerna.reactor.AbstractReactor;
import prerna.reactor.agent.run.AgentRunRecord;
import prerna.reactor.agent.run.AgentRunRequest;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.security.HttpHelperUtility;
import prerna.util.Utility;

/**
 * Posts a callback for the agent run currently executing on this thread.
 *
 * <p>
 * This reactor intentionally takes no Pixel arguments. Agent run workers seed
 * {@link ThreadStore#getJobId()} with the durable run id, so the reactor can
 * reload the persisted {@link AgentRunRequest} and obtain its
 * {@code agentParams}. Configure it as an {@code afterRun} Pixel hook:
 *
 * <pre>
 * PostAgentRunCallback();
 * </pre>
 *
 * Pass the callback configuration when submitting the run:
 *
 * <pre>
 * agentParams=[{
 *   "callbackUrl": "https://example.test/agent-complete",
 *   "correlationId": "request-123"
 * }]
 * </pre>
 *
 * The callback body contains {@code runId}, {@code roomId},
 * {@code workspaceId}, and an {@code agentParams} object containing every
 * supplied entry except the reserved {@code callbackUrl} field.
 */
public class PostAgentRunCallbackReactor extends AbstractReactor {

	private static final Gson GSON = new Gson();
	private static final String CALLBACK_URL = "callbackUrl";

	public PostAgentRunCallbackReactor() {
		this.keysToGet = new String[] {};
		this.keyRequired = new int[] {};
	}

	@Override
	public NounMetadata execute() {
		String runId = StringUtils.trimToNull(ThreadStore.getJobId());
		if (runId == null) {
			throw new IllegalStateException("PostAgentRunCallback must execute inside an agent run hook");
		}

		AgentRunRecord run = AgentRunStore.getRun(runId, this.insight);
		if (run == null || run.request() == null) {
			throw new IllegalStateException("Could not load the current agent run request for runId=" + runId);
		}

		AgentRunRequest request = run.request();
		// Hooks also run for delegated/subagent runs. Only the root RunAgent call
		// owns the pass-through callback contract; firing for a child would either
		// fail (children do not inherit agentParams) or notify the caller too early.
		if (StringUtils.isNotBlank(request.getParentRunId())) {
			return new NounMetadata("Skipped callback for child agent run " + run.runId(),
					PixelDataType.CONST_STRING);
		}

		// Work on a copy so removing the reserved transport field cannot mutate
		// the persisted request's agentParams map.
		Map<String, Object> agentParams = new LinkedHashMap<>(request.getAgentParamMap());
		String callbackUrl = StringUtils.trimToNull(stringValue(agentParams.remove(CALLBACK_URL)));
		if (callbackUrl == null) {
			throw new IllegalArgumentException(
					"RunAgent agentParams must contain a non-empty '" + CALLBACK_URL + "'");
		}

		Utility.checkIfValidDomain(callbackUrl);

		Map<String, Object> body = new LinkedHashMap<>();
		body.put("runId", run.runId());
		body.put("roomId", run.roomId());
		body.put("workspaceId", request.getWorkspaceId());
		body.put("agentParams", agentParams);

		String response = HttpHelperUtility.postRequestStringBody(callbackUrl, null, GSON.toJson(body),
				ContentType.APPLICATION_JSON, null, null, null);
		return new NounMetadata(response == null ? "" : response, PixelDataType.CONST_STRING);
	}

	private static String stringValue(Object value) {
		return value == null ? null : String.valueOf(value);
	}

	@Override
	public String getReactorDescription() {
		return "POST the current agent run identity and pass-through agentParams to agentParams.callbackUrl. "
				+ "This no-argument reactor is intended for an afterRun Pixel hook.";
	}
}
