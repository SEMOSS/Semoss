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

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;

public class WorkflowQuartzJob implements InterruptableJob {

	private final AtomicBoolean interrupted = new AtomicBoolean(false);

	@Override
	public void execute(JobExecutionContext context) {

		WorkflowDefinition wf = (WorkflowDefinition) context.getMergedJobDataMap().get("workflow");

		File file = (File) context.getMergedJobDataMap().get("file");

		String executionId = context.getMergedJobDataMap().getString("executionId");

		/*
		 * ExecutionContext ctx = new ExecutionContext(); ctx.input.put("file", file);
		 * ctx.input.put("executionId", executionId); ctx.input.put("interruptFlag",
		 * interrupted);
		 * 
		 * new WorkflowEngine().run(wf, ctx);
		 */
	}

	@Override
	public void interrupt() {
		interrupted.set(true);
		System.out.println(" Workflow job interrupted");
	}
}
