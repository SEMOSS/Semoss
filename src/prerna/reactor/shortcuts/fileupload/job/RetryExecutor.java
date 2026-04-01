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
package prerna.reactor.shortcuts.fileupload.job;

import java.util.concurrent.CompletableFuture;

import prerna.om.Insight;

public class RetryExecutor {
	public static CompletableFuture<ExecutionContext> executeWithRetry(String pixel, ExecutionContext ctx,
			RetryPolicy retryPolicy, String workflowId, String nodeId, Insight insight) {

		return CompletableFuture.supplyAsync(() -> {

			int attempt = 0;
			long delay = retryPolicy.getInitialDelayMs();

			while (attempt <= retryPolicy.getMaxRetries()) {

				try {

					try {
						return ActionService.execute(insight, pixel, ctx);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				} catch (Exception ex) {

					attempt++;

					if (attempt > retryPolicy.getMaxRetries()) {

						DLQService.sendToDLQ(workflowId, nodeId, ex, ctx);

						throw new RuntimeException(ex);
					}

					try {

						Thread.sleep(delay);

					} catch (InterruptedException ignored) {
					}

					delay = Math.min((long) (delay * retryPolicy.getMultiplier()), retryPolicy.getMaxDelayMs());
				}
			}

			return null;
		});
	}
}
