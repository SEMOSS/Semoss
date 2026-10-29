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
package prerna.reactor;

import java.util.Objects;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.contract.Invocation;
import prerna.contract.Key;
import prerna.contract.Output;
import prerna.contract.Reactor;
import prerna.reactor.agent.AgentCancelHook;
import prerna.sablecc2.comm.JobStreamEnvelopes;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobManager.InterruptResult;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.tcp.client.SocketClient;

@Reactor(name = "StopPixelExecution", description = "Stop the current execution of a pixel job")
@Invocation(mode = Invocation.Mode.BOTH)
public class StopPixelExecutionReactor extends AbstractReactor2<StopPixelExecutionReactor.Input, String> {

	public record Input(
			@Key(description = "The id for the job. If running the pixel synchronously, the job id will be the same as the insight id.")
			String id) {}

	@Override
	@Output(name = "message",
			description = "A message describing the cancellation outcome. The caller must own an existing job or be an administrator.")
	protected String execute(Input input) {
		String jobId = input.id();
		PixelJobManager jobManager = PixelJobManager.getManager();
		PixelJobRunner jobRunner = jobManager.getJob(jobId);
		if (jobRunner == null) {
			return "Pixel job " + jobId + " not found";
		}

		User user = this.insight.getUser();
		User jobUser = jobRunner.getInsight() == null ? null : jobRunner.getInsight().getUser();
		boolean isOwner = user != null && jobUser != null
				&& Objects.equals(User.getSingleLogginName(user), User.getSingleLogginName(jobUser));
		boolean isAdmin = user != null && SecurityAdminUtils.userIsAdmin(user);
		if (!isOwner && !isAdmin) {
			throw new IllegalArgumentException("User does not have permission to stop this pixel job");
		}

		String insightId = null;
		if (jobRunner != null && jobRunner.getInsight() != null) {
			insightId = jobRunner.getInsight().getInsightId();
		}

		// generic terminal envelope so any subscriber sees cancel without waiting on
		// status polling
		JobStreamEnvelopes.jobCancelled(jobId, "user-requested");

		InterruptResult interruptResult = jobManager.interruptThread(jobId);

		SocketClient pySocketClient = user == null ? null : user.getPythonSocketClient(false);
		if (pySocketClient != null && (insightId != null || jobId != null)) {
			pySocketClient.interruptInsightJob(insightId, jobId);
		}

		// agent-aware extras (subagent cascade + CLI sidecar interrupt); no-op for
		// non-agent jobs
		AgentCancelHook.onStop(jobId);

		final PixelJobRunner jobThread = jobRunner;
		Thread cleanupThread = new Thread(() -> {
			try {
				jobThread.joinExecution();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				jobManager.clearJob(jobId);
				jobManager.removeJob(jobId);
			}
		}, "pixel-job-cleanup-" + jobId);
		cleanupThread.setDaemon(true);
		cleanupThread.start();

		String message;
		if (interruptResult == InterruptResult.CANCEL_REQUESTED) {
			message = "Cancel requested for pixel job " + jobId;
		} else if (interruptResult == InterruptResult.ALREADY_DONE) {
			message = "Pixel job " + jobId + " already completed";
		} else {
			message = "Pixel job " + jobId + " not found";
		}
		return message;
	}

}
