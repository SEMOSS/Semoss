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
package prerna.util.openshell;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import prerna.util.OpenShellTransport;
import prerna.util.Utility;

/**
 * Deletes executor sandboxes that outlived their session.
 *
 * <p>
 * Normal teardown is the logout path: {@code UserSessionLoader.sessionDestroyed}
 * → {@code ClientProcessWrapper.shutdown(true)} → {@code OpenShellTransport.close()}.
 * That covers a user who logs out or whose session times out. It does not cover
 * the cases that actually leak:
 *
 * <ul>
 * <li>Tomcat is killed or crashes — no session event is ever delivered;</li>
 * <li>the sandbox is created and the JVM dies before it is recorded anywhere;</li>
 * <li>a session is abandoned in a way the container never notices.</li>
 * </ul>
 *
 * <p>
 * So this is a backstop, not the primary mechanism: it lists only sandboxes
 * carrying the executor label <b>and</b> the {@code pyw-} name prefix, and only
 * deletes those older than a threshold. Age is the only signal that is safe to
 * act on without a live registry of owners: a young sandbox may belong to a
 * session on another replica, and killing it would be a real user-visible bug.
 *
 * <p>
 * Run it from a timer, not from the request path:
 *
 * <pre>
 * java -cp ... prerna.util.openshell.OpenShellSandboxReaper --max-age-hours 12
 * java -cp ... prerna.util.openshell.OpenShellSandboxReaper --max-age-hours 12 --dry-run
 * </pre>
 */
public final class OpenShellSandboxReaper {

	private static final DateTimeFormatter LIST_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private OpenShellSandboxReaper() {
		// static utility
	}

	public static void main(String[] args) {
		String rdf = System.getProperty("RDF_MAP", "/opt/semosshome/RDF_Map.prop");
		prerna.util.DIHelper.getInstance().loadCoreProp(rdf);

		double maxAgeHours = 12;
		boolean dryRun = false;
		String label = Utility.getDIHelperProperty(OpenShellTransport.LABEL_PROP);
		if (label == null || label.trim().isEmpty()) {
			label = "app=semoss-executor";
		}
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "--max-age-hours":
				maxAgeHours = Double.parseDouble(args[++i]);
				break;
			case "--dry-run":
				dryRun = true;
				break;
			case "--label":
				label = args[++i];
				break;
			default:
				System.out.println("unknown argument: " + args[i]);
				System.exit(2);
			}
		}

		System.out.println("label=" + label + " maxAgeHours=" + maxAgeHours + (dryRun ? " (dry run)" : ""));
		int reaped = reap(label, maxAgeHours, dryRun);
		System.out.println((dryRun ? "would reap " : "reaped ") + reaped + " sandbox(es)");
	}

	/**
	 * Delete labelled, prefixed sandboxes older than {@code maxAgeHours}.
	 *
	 * @return how many were (or would be) deleted
	 */
	public static int reap(String label, double maxAgeHours, boolean dryRun) {
		List<String[]> candidates = listExecutorSandboxes(label);
		LocalDateTime cutoff = LocalDateTime.now().minus(Duration.ofMinutes((long) (maxAgeHours * 60)));
		int count = 0;
		for (String[] row : candidates) {
			String name = row[0];
			String created = row[1];
			LocalDateTime timestamp;
			try {
				timestamp = LocalDateTime.parse(created, LIST_TIMESTAMP);
			} catch (Exception e) {
				// Unparseable age: skip rather than guess. Deleting a sandbox we cannot
				// date is exactly the mistake this class exists to avoid.
				System.out.println("  skip " + name + " (unparseable timestamp '" + created + "')");
				continue;
			}
			if (timestamp.isAfter(cutoff)) {
				continue;
			}
			count++;
			if (dryRun) {
				System.out.println("  would delete " + name + " (created " + created + ")");
			} else {
				System.out.println("  deleting " + name + " (created " + created + ")");
				run("openshell", "sandbox", "delete", name);
			}
		}
		return count;
	}

	/**
	 * List sandboxes carrying the executor label whose name matches the executor
	 * prefix. Two independent filters on purpose: the label says "this gateway
	 * knows about executors", the prefix says "this process created it".
	 */
	private static List<String[]> listExecutorSandboxes(String label) {
		List<String[]> rows = new ArrayList<>();
		String output = run("openshell", "sandbox", "list", "--selector", label);
		for (String line : output.split("\\R")) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("NAME")) {
				continue;
			}
			String[] columns = trimmed.split("\\s+");
			if (columns.length < 3) {
				continue;
			}
			String name = columns[0];
			if (!name.startsWith("pyw-")) {
				continue;
			}
			rows.add(new String[] { name, columns[1] + " " + columns[2] });
		}
		return rows;
	}

	private static String run(String... command) {
		try {
			Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
			String output = new String(process.getInputStream().readAllBytes());
			process.waitFor();
			return output;
		} catch (Exception e) {
			System.out.println("  command failed: " + String.join(" ", command) + " -> " + e.getMessage());
			return "";
		}
	}
}
