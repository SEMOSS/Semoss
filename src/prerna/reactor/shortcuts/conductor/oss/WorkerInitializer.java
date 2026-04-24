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
package prerna.reactor.shortcuts.conductor.oss;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.netflix.conductor.client.http.ConductorClient;
import com.netflix.conductor.client.http.TaskClient;
import com.netflix.conductor.client.worker.Worker;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskResult;

public class WorkerInitializer {
	private final ExecutorService executor;
	private volatile boolean running = true;

	public WorkerInitializer(int threadPoolSize) {
		this.executor = Executors.newFixedThreadPool(threadPoolSize);
	}

	public void startWorkers(List<Worker> workers) {

		ConductorClient conductorClient = new ConductorClient("http://localhost:9191/api");

		for (Worker worker : workers) {

			// One thread per worker type (not nested loop)
			executor.submit(() -> pollLoop(worker, conductorClient));
		}

		System.out.println("Worker polling started...");
	}

	private void pollLoop(Worker worker, ConductorClient conductorClient) {

		// Each thread gets its own TaskClient (important)
		TaskClient taskClient = new TaskClient(conductorClient);

		String taskType = worker.getTaskDefName();
		String workerId = "worker-" + taskType;

		while (running && !Thread.currentThread().isInterrupted()) {

			try {
				Task task = taskClient.pollTask(taskType, workerId, null);

				if (task != null && task.getTaskId() != null) {

					TaskResult result = worker.execute(task);

					taskClient.updateTask(result);
				} else {
					// No task - wait a bit (reduce CPU usage)
					Thread.sleep(500);
				}

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				System.out.println("Worker interrupted: " + taskType);
			} catch (Exception e) {
				System.err.println("Error in worker " + taskType + ": " + e.getMessage());
			}
		}
	}

	public void stop() {
		running = false;
		executor.shutdown();

		try {
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
		}

		System.out.println("Workers stopped.");
	}

}
