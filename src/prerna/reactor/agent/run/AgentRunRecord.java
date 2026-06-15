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
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *******************************************************************************/
package prerna.reactor.agent.run;

public final class AgentRunRecord {

	private final String runId;
	private final String roomId;
	private final AgentRunStatus status;
	private final RunAgentRequest request;
	private final String userId;
	private final String jobId;

	public AgentRunRecord(String runId, String roomId, AgentRunStatus status, RunAgentRequest request, String userId,
			String jobId) {
		this.runId = runId;
		this.roomId = roomId;
		this.status = status;
		this.request = request;
		this.userId = userId;
		this.jobId = jobId;
	}

	public String getRunId() {
		return runId;
	}

	public String getRoomId() {
		return roomId;
	}

	public AgentRunStatus getStatus() {
		return status;
	}

	public RunAgentRequest getRequest() {
		return request;
	}

	public String getUserId() {
		return userId;
	}

	public String getJobId() {
		return jobId;
	}
}
