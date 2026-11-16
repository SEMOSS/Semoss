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
package prerna.reactor.mgmt;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;
import prerna.util.Settings;
import prerna.util.Utility;

public class MgmtUtil {

	protected static final Logger classLogger = LogManager.getLogger(MgmtUtil.class);

	static long previousTime = 0;
	static SystemInfo si = new SystemInfo();
	static OperatingSystem os = si.getOperatingSystem();
	static GlobalMemory gm = si.getHardware().getMemory();
	static AtomicLong freeMemory = new AtomicLong(-1);
	static AtomicLong userCount = new AtomicLong(0);

	public static void diskUtilizationPerProcess(int pid) {
		/**
		 * ByteRead : Returns the number of bytes the process has read from disk.
		 * ByteWritten : Returns the number of bytes the process has written to disk.
		 */
		OSProcess process;
		process = os.getProcess(pid);
		classLogger.info("Disk I/O Usage :");
		classLogger.info("I/O Reads: {}", process.getBytesRead());
		classLogger.info("I/O Writes: {}", process.getBytesWritten());
	}

	public static void cpuUtilizationPerProcess(int processId) {
		/**
		 * User Time : Returns the number of milliseconds the process has executed in
		 * user mode. Kernel Time : Returns the number of milliseconds the process has
		 * executed in kernel/system mode.
		 */
		SystemInfo systemInfo = new SystemInfo();
		CentralProcessor processor = systemInfo.getHardware().getProcessor();
		int cpuNumber = processor.getLogicalProcessorCount();
		// int processId = systemInfo.getOperatingSystem().getProcessId();
		OSProcess process = systemInfo.getOperatingSystem().getProcess(processId);

		long currentTime = process.getKernelTime() + process.getUserTime();
		long timeDifference = currentTime - previousTime;
		double processCpu = (100 * (timeDifference / 5000d)) / cpuNumber;
		previousTime = currentTime;
		classLogger.info("CPU Usage :");
		classLogger.info("CPU : {}%", (int) processCpu);
	}

	public static long memoryUtilizationPerProcess(int pid) {
		/**
		 * Resident Size : how much memory is allocated to that process and is in RAM
		 */
		OSProcess process;

		process = os.getProcess(pid);
		if (process == null) {
			return 0;
		}
		return process.getResidentSetSize();
	}

	/**
	 * Returns the operating system process id of a process.
	 *
	 * @param p the process
	 * @return the process id, or -1 when the process is null
	 */
	public static int getProcessID(Process p) {
		if (p == null) {
			return -1;
		}
		return (int) p.pid();
	}

	public static void printChild(int pid) {
		List<OSProcess> childProcesses = os.getChildProcesses(pid, null, null, 10);

		for (int childIndex = 0; childIndex < childProcesses.size(); childIndex++) {
			classLogger.info("Process id {} <> {}", childProcesses.get(childIndex).getProcessID(),
					childProcesses.get(childIndex).getCommandLine());
		}
	}

	public static long getFreeMemory() {
		if (freeMemory.longValue() == -1) {
			long availableMemory = gm.getAvailable();
			// convert to gigs
			freeMemory.set(availableMemory / (1024 * 1024 * 1024));
			classLogger.info("Server total available memory = {} GB", freeMemory.longValue());
			// give a 2GB limit
			String reservedJavaMem = Utility.getDIHelperProperty(Settings.RESERVED_JAVA_MEM);
			if (reservedJavaMem != null && !(reservedJavaMem = reservedJavaMem.trim()).isEmpty()) {
				long javaReservedMemory = Long.parseLong(reservedJavaMem);
				freeMemory.getAndAdd(-1 * javaReservedMemory);
				classLogger.info("Total available memory minus RESERVED_JAVA_MEM = {} GB", freeMemory.longValue());

			}
		}
		return freeMemory.longValue();
	}

	public static long getAFreeMemory() {
		return gm.getAvailable();
	}

	public static long getTotalMemory() {
		return gm.getTotal();
	}

	/**
	 * Returns the ids of the processes listening on a port, read from the output of
	 * netstat on Windows and lsof elsewhere.
	 *
	 * @param port the port
	 * @return the process ids, empty when nothing listens on the port or they
	 *         cannot be read
	 */
	public static List<Integer> getPidsByPort(int port) {
		List<String> command = SystemUtils.IS_OS_WINDOWS ? List.of("netstat", "-ano")
				: List.of("lsof", "-t", "-i:" + port, "-sTCP:LISTEN");
		List<Integer> pids = new ArrayList<>();
		try {
			Process process = new ProcessBuilder(command).redirectError(ProcessBuilder.Redirect.DISCARD).start();
			List<String> lines;
			try (BufferedReader reader = process.inputReader(StandardCharsets.UTF_8)) {
				lines = reader.lines().toList();
			}
			process.waitFor(7L, TimeUnit.SECONDS);
			for (String line : lines) {
				String pid = SystemUtils.IS_OS_WINDOWS ? getWindowsListenerPid(line, port) : line.trim();
				if (pid == null || pid.isEmpty()) {
					continue;
				}
				try {
					pids.add(Integer.parseInt(pid));
				} catch (NumberFormatException e) {
					classLogger.warn("Skipping '{}', which is not a process id, in the listeners of port {}",
							Utility.cleanLogString(pid), port);
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to find the processes listening on port {}", port, e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			classLogger.error("Interrupted while finding the processes listening on port {}", port, e);
		}
		return pids;
	}

	/**
	 * Returns the id of the first process listening on a port.
	 *
	 * @param port the port
	 * @return the process id, or -1 when nothing listens on the port
	 */
	public static int getPidByPort(int port) {
		List<Integer> pids = getPidsByPort(port);
		return pids.isEmpty() ? -1 : pids.get(0);
	}

	/**
	 * Returns the process id in a line of Windows netstat output when the line is a
	 * listener on the port.
	 *
	 * @param line the netstat line
	 * @param port the port
	 * @return the process id, or null when the line is not a listener on the port
	 */
	private static String getWindowsListenerPid(String line, int port) {
		if (!line.contains("LISTENING")) {
			return null;
		}
		String[] columns = line.trim().split("\\s+");
		if (columns.length < 5 || !columns[1].endsWith(":" + port)) {
			return null;
		}
		return columns[4];
	}

	public static void removeMemory4User(long memoryInGigs) {
		// remove the user
		// add the memory back
		userCount.decrementAndGet();
		classLogger.info("User logging out releasing {} GB of memory", memoryInGigs);
		freeMemory.addAndGet(memoryInGigs);
		classLogger.info("Total available memory after removing user = {} GB", freeMemory.longValue());
	}

	public static void addMemory4User(long memoryInGigs) {
		// add number of users
		// remove the memory
		userCount.incrementAndGet();
		classLogger.info("User logging in consuming {} GB of memory", memoryInGigs);
		freeMemory.addAndGet(-1 * memoryInGigs);
		classLogger.info("Total available memory after adding user = {} GB", freeMemory.longValue());
	}

	public static long getUserCount() {
		return userCount.longValue();
	}

}
