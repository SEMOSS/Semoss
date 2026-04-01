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

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.schedules.ScheduleClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class TemporalClientProvider {

	private static final String TARGET = "127.0.0.1:7233";
	private static final String NAMESPACE = "default";
	private static final String TASK_QUEUE = "WORKFLOW_TASK_QUEUE";

	private static WorkflowServiceStubs service;
	private static WorkflowClient client;
	private static WorkerFactory factory;
	private static boolean initialized = false;

	private TemporalClientProvider() {
	}

	public static synchronized void init() {
		if (initialized) {
			return;
		}

		WorkflowServiceStubsOptions serviceOptions = WorkflowServiceStubsOptions.newBuilder().setTarget(TARGET).build();

		service = WorkflowServiceStubs.newServiceStubs(serviceOptions);

		WorkflowClientOptions clientOptions = WorkflowClientOptions.newBuilder().setNamespace(NAMESPACE).build();

		client = WorkflowClient.newInstance(service, clientOptions);

		factory = WorkerFactory.newInstance(client);

		Worker worker = factory.newWorker(TASK_QUEUE);
		worker.registerWorkflowImplementationTypes(WorkflowEngineImpl.class);
		worker.registerActivitiesImplementations(new WorkflowActivityImpl());

		factory.start();
		initialized = true;

		System.out.println("Temporal initialized successfully");
	}

	public static WorkflowClient getClient() {
		if (!initialized) {
			throw new IllegalStateException("Temporal not initialized");
		}
		return client;
	}

	public static WorkflowServiceStubs getService() {
		if (!initialized) {
			throw new IllegalStateException("Temporal not initialized");
		}
		return service;
	}

	public static ScheduleClient getScheduleClient() {
		if (!initialized) {
			throw new IllegalStateException("Temporal not initialized");
		}
		return ScheduleClient.newInstance(service);
	}
}
