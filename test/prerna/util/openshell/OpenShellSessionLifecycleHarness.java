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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import prerna.om.ClientProcessWrapper;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.SocketClient;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OpenShellTransport;
import prerna.util.SymlinkHelper;
import prerna.util.Utility;

/**
 * Proves the sandbox lifecycle that login/logout is supposed to give us,
 * without booting Tomcat.
 *
 * <p>
 * The claim being tested is not "a sandbox can be created" — the transport
 * harness already shows that. It is the <b>session-shaped</b> claim:
 *
 * <ol>
 * <li>nothing exists while the user has not run any code;</li>
 * <li>the first user-authored python request creates the sandbox on demand;</li>
 * <li>{@code Py()} actually executes inside it (real worker, not a stock image);</li>
 * <li>the exact call made on logout — {@code ClientProcessWrapper.shutdown(true)}
 * from {@code UserSessionLoader.sessionDestroyed} — deletes it.</li>
 * </ol>
 *
 * <p>
 * Step 4 is the one worth automating: it is a single call buried in a servlet
 * listener, and if the wrapper forgets its transport, logout silently leaks a
 * sandbox.
 *
 * <p>
 * Run with an RDF map whose {@code SANDBOX_MODE} is {@code OPENSHELL}:
 *
 * <pre>
 * java -DRDF_MAP=/tmp/RDF_Map.openshell-harness.prop -cp ... \
 *      prerna.util.openshell.OpenShellSessionLifecycleHarness
 * </pre>
 */
public final class OpenShellSessionLifecycleHarness {

	private static int passed = 0;
	private static int failed = 0;

	public static void main(String[] args) throws Exception {
		String rdf = System.getProperty("RDF_MAP");
		if (rdf == null || rdf.trim().isEmpty()) {
			System.err.println("Set -DRDF_MAP=<path to an RDF_Map.prop with SANDBOX_MODE=OPENSHELL>");
			System.exit(2);
		}
		DIHelper.getInstance().loadCoreProp(rdf);
		System.out.println("RDF: " + rdf);
		String mode = Utility.getDIHelperProperty(Constants.SANDBOX_MODE);
		String image = Utility.getDIHelperProperty(OpenShellTransport.IMAGE_PROP);
		String policy = Utility.getDIHelperProperty(OpenShellTransport.POLICY_PROP);
		System.out.println("SANDBOX_MODE=" + mode + "  image=" + image + "  policy=" + policy);
		check("P-0 SANDBOX_MODE is OPENSHELL", "OPENSHELL".equalsIgnoreCase(mode), String.valueOf(mode));
		check("P-1 image is configured", image != null && !image.trim().isEmpty(), String.valueOf(image));
		check("P-2 policy is configured", policy != null && !policy.trim().isEmpty(), String.valueOf(policy));
		check("P-3 CHROOT_ENABLE is on", Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE)),
				String.valueOf(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE)));
		if (failed > 0) {
			System.out.println("aborting: the properties above decide whether this test means anything");
			System.exit(1);
		}

		String chrootRoot = Utility.getDIHelperProperty(Constants.CHROOT_DIR);
		String chrootPath = chrootRoot + File.separator + "lifecycle-" + UUID.randomUUID().toString().substring(0, 8);
		Files.createDirectories(Paths.get(chrootPath));
		String sandboxName = OpenShellTransport.sandboxNameFor(chrootPath);
		System.out.println("chroot:  " + chrootPath);
		System.out.println("sandbox: " + sandboxName);

		ClientProcessWrapper cpw = new ClientProcessWrapper();
		try {
			check("L-1 no sandbox before first use", !sandboxExists(sandboxName), sandboxName);

			// This is the same call the first Py() makes: User.getPythonSocketClient(true)
			// -> ClientProcessWrapper.createProcessAndClient(...) -> OpenShellTransport.start().
			System.out.println("\nstarting on demand (this creates the sandbox)...");
			long started = System.currentTimeMillis();
			SymlinkHelper helper = new SymlinkHelper(chrootPath);
			check("L-2 helper selected the external sandbox", helper.isExternalSandbox(), helper.getSandboxMode().name());
			cpw.createProcessAndClient(true, helper, -1, null, null, null, false, "30", "INFO", null);
			System.out.println("  up in " + (System.currentTimeMillis() - started) + " ms");
			check("L-3 sandbox exists after start", sandboxExists(sandboxName), sandboxName);

			// The real point: user-authored python runs in the sandbox, through the
			// shipping client, not a probe socket.
			SocketClient client = cpw.getSocketClient();
			check("L-4 socket client connected", client != null && client.isConnected(), "client=" + client);
			if (client != null && client.isConnected()) {
				PayloadStruct ps = new PayloadStruct();
				ps.operation = PayloadStruct.OPERATION.PYTHON;
				ps.methodName = "lifecycleHarness";
				ps.payload = new Object[] { "import os,sys; print('LIFECYCLE_OK', os.getpid(), sys.version.split()[0])" };
				ps.payloadClasses = new Class[] { String.class };
				ps.longRunning = true;
				ps.response = false;
				ps.interim = false;
				ps.insightId = "lifecycle-harness";
				ps.executionInsightId = "lifecycle-harness";
				ps.sessionId = "lifecycle-harness";
				ps.jobId = "lifecycle-harness";
				Object reply = client.executeCommand(ps);
				String text;
				if (reply instanceof PayloadStruct) {
					PayloadStruct replyStruct = (PayloadStruct) reply;
					text = java.util.Arrays.deepToString(replyStruct.payload);
					System.out.println("  response=" + replyStruct.response + " payload=" + text);
				} else {
					text = String.valueOf(reply);
					System.out.println("  reply=" + text);
				}
				check("L-5 Py() executed inside the sandbox", text.contains("LIFECYCLE_OK"), abbreviate(text));
			}

			// Exactly what UserSessionLoader.sessionDestroyed does for the session user.
			System.out.println("\nshutting down as logout does (cpw.shutdown(true))...");
			long stopped = System.currentTimeMillis();
			cpw.shutdown(true);
			System.out.println("  down in " + (System.currentTimeMillis() - stopped) + " ms");
			Thread.sleep(2000);
			check("L-6 sandbox deleted on logout", !sandboxExists(sandboxName), sandboxName);

			// The second half of the on-demand claim: after teardown the same wrapper
			// can start a fresh sandbox, so a new session does not inherit a dead one.
			System.out.println("\nstarting again (second session)...");
			cpw.createProcessAndClient(true, new SymlinkHelper(chrootPath), -1, null, null, null, false, "30", "INFO",
					null);
			check("L-7 a later session starts a fresh sandbox", sandboxExists(sandboxName), sandboxName);
		} catch (Exception e) {
			check("L-0 lifecycle completed without exception", false, e.getClass().getSimpleName() + ": " + e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				cpw.shutdown(true);
			} catch (Exception ignored) {
				// best effort
			}
			Thread.sleep(1000);
			if (sandboxExists(sandboxName)) {
				System.out.println("cleaning up leftover sandbox " + sandboxName);
				deleteSandbox(sandboxName);
			}
			deleteRecursively(Paths.get(chrootPath));
		}

		int total = passed + failed;
		System.out.println("\n=== " + (failed == 0 ? "ALL PASS" : "FAILURES") + " (" + passed + "/" + total
				+ " checks passed) ===");
		if (failed > 0) {
			System.exit(1);
		}
	}

	// ------------------------------------------------------------------
	// helpers
	// ------------------------------------------------------------------

	private static boolean sandboxExists(String name) {
		String output = run("openshell", "sandbox", "list");
		for (String line : output.split("\\R")) {
			String trimmed = line.trim();
			if (trimmed.startsWith(name + " ") || trimmed.equals(name)) {
				return true;
			}
		}
		return false;
	}

	private static void deleteSandbox(String name) {
		run("openshell", "sandbox", "delete", name);
	}

	private static String run(String... command) {
		try {
			Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
			String output = new String(process.getInputStream().readAllBytes());
			process.waitFor();
			return output;
		} catch (Exception e) {
			return "";
		}
	}

	private static void deleteRecursively(Path root) {
		if (!Files.exists(root)) {
			return;
		}
		try (var stream = Files.walk(root)) {
			stream.sorted(java.util.Comparator.reverseOrder()).forEach(path -> {
				try {
					Files.deleteIfExists(path);
				} catch (Exception ignored) {
					// best effort
				}
			});
		} catch (Exception ignored) {
			// best effort
		}
	}

	private static String abbreviate(String text) {
		if (text == null) {
			return "";
		}
		return text.length() <= 120 ? text : text.substring(0, 117) + "...";
	}

	private static void check(String name, boolean ok, String detail) {
		if (ok) {
			passed++;
		} else {
			failed++;
		}
		System.out.printf("  %s %-48s %s%n", ok ? "[PASS]" : "[FAIL]", name, detail == null ? "" : detail);
	}
}
