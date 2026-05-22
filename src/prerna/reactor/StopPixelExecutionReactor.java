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

import prerna.auth.User;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.comm.PixelJobManager.InterruptResult;
import prerna.sablecc2.comm.PixelJobRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.tcp.client.SocketClient;

public class StopPixelExecutionReactor extends AbstractReactor {

	public StopPixelExecutionReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String jobId = this.keyValue.get(ReactorKeysEnum.ID.getKey());
		PixelJobManager jobManager = PixelJobManager.getManager();
		PixelJobRunner jobRunner = jobManager.getJob(jobId);
		String insightId = null;
		if (jobRunner != null && jobRunner.getInsight() != null) {
			insightId = jobRunner.getInsight().getInsightId();
		}

		InterruptResult interruptResult = jobManager.interruptThread(jobId);

		User user = this.insight.getUser();
		SocketClient pySocketClient = user == null ? null : user.getPythonSocketClient(false);
		if (pySocketClient != null && (insightId != null || jobId != null)) {
			pySocketClient.interruptInsightJob(insightId, jobId);
		}

		if (jobRunner == null) {
			jobManager.clearJob(jobId);
		} else {
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
		}

		String message;
		if (interruptResult == InterruptResult.CANCEL_REQUESTED) {
			message = "Cancel requested for pixel job " + jobId;
		} else if (interruptResult == InterruptResult.ALREADY_DONE) {
			message = "Pixel job " + jobId + " already completed";
		} else {
			message = "Pixel job " + jobId + " not found";
		}
		return new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Stop the current execution of a pixel job";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equalsIgnoreCase(ReactorKeysEnum.ID.getKey())) {
			return "The id for the job. If running the pixel synchronously, the job id will be the same as the insight id.";
		}
		return super.getDescriptionForKey(key);
	}
}
