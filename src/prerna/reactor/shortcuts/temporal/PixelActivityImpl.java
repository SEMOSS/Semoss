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
package prerna.reactor.shortcuts.temporal;

import java.util.Map;

import prerna.om.Insight;
import prerna.reactor.scheduler.SchedulerDatabaseUtility;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PixelActivityImpl implements PixelActivity {

	Object result = null;

	@Override
	public Object runPixel(Insight insight, PixelRequest pixelRequest) {
		try {

			PixelRunner innerRunner = insight.runPixel(pixelRequest.getPixel());

			NounMetadata nounMetadata = innerRunner.getResults().get(0);
			Map<String, Object> resultMap = (Map<String, Object>) innerRunner.getResults().get(0).getValue();
			String key = resultMap.keySet().stream().findFirst().orElse(null);
			insight.getVarStore().put(key, nounMetadata);

			// Save success
			SchedulerDatabaseUtility.insertActionExecution(pixelRequest.getWorkflowExecutionId(),
					pixelRequest.getActionId(), pixelRequest.getActionName(), "SUCCESS", pixelRequest.getPixel(),
					pixelRequest.getContext().toString(), result != null ? result.toString() : null, null);

			return resultMap;

		} catch (Exception e) {

			try {
				SchedulerDatabaseUtility.insertActionExecution(pixelRequest.getWorkflowExecutionId(),
						pixelRequest.getActionId(), pixelRequest.getActionName(), "FAILED", pixelRequest.getPixel(),
						pixelRequest.getContext().toString(), result != null ? result.toString() : null,
						e.getMessage());

				SchedulerDatabaseUtility.insertWorkflowDLQ(pixelRequest.getWorkflowExecutionId(),
						pixelRequest.getWorkflowKey(), pixelRequest.getActionId(), pixelRequest.getPixel(),
						e.getMessage(), 0, 3, "FAILED");
			} catch (Exception dbException) {
				// Log database error but don't swallow the original exception
				dbException.printStackTrace();
			}

			// Always re-throw the original activity failure
			throw new RuntimeException("Pixel execution failed for action " + pixelRequest.getActionId() + ": " + e.getMessage(), e);
		}
	}

}
