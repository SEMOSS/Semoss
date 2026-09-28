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
package prerna.util;

import prerna.tcp.PayloadStruct;
import prerna.tcp.client.NativePySocketClient;

/**
 * Drives the REAL client ({@link NativePySocketClient}) end to end against a sandbox
 * created by {@link OpenShellTransport} -- connect, send, and read the reply.
 *
 * <p>
 * An earlier version only proved the handshake ({@code ready=true}). This one sends an
 * actual {@link PayloadStruct} and waits for the response, which is the operation
 * {@code PyTranslator} performs and the one the live pixel fails on. It carries no
 * protocol code of its own: everything goes through the class that ships, because
 * debugging through a hand-rolled protocol harness meant repeatedly mistaking a
 * failing test client for a failing transport.
 *
 * <p>
 * Usage: {@code java ... prerna.util.OpenShellNativeClientHarness [sessionKey]}
 */
public final class OpenShellNativeClientHarness {

	public static void main(String[] args) throws Exception {
		String rdf = System.getProperty("RDF_MAP", "/opt/semosshome/RDF_Map.prop");
		DIHelper.getInstance().loadCoreProp(rdf);
		String sessionKey = args.length > 0 ? args[0] : "native-client-1";

		OpenShellTransport t = OpenShellTransport.forSession(sessionKey);
		int port = t.start();
		System.out.println("transport up on port " + port);

		// Wait for the worker WITHOUT touching its port: a probe connect would consume
		// the single connection slot and its disconnect would kill the worker.
		System.out.println("settling 5s...");
		Thread.sleep(5000);

		NativePySocketClient client = new NativePySocketClient(java.util.Map.of());
		client.connect("127.0.0.1", port, false);
		Thread reader = new Thread(client, "native-client-reader");
		reader.setDaemon(true);
		reader.start();
		client.awaitReadyOrKill(30_000, 500);
		System.out.println("connected=" + client.isConnected() + " ready=" + client.isReady());

		System.out.println("--- sending a real PayloadStruct (what PyTranslator sends) ---");
		PayloadStruct ps = new PayloadStruct();
		ps.epoc = "00000000000000000001";
		ps.operation = PayloadStruct.OPERATION.PYTHON;
		ps.methodName = "transportScriptWithExplicitPaths";
		ps.payload = new Object[] { "import os; print('NATIVE_CLIENT_OK', os.getpid())" };
		ps.payloadClasses = new Class[] { String.class };
		ps.longRunning = true;
		ps.hasReturn = true;
		ps.insightId = "harness-insight";
		ps.executionInsightId = "harness-insight";
		ps.sessionId = "harness-session";
		ps.jobId = "harness-job";

		try {
			Object resp = client.executeCommand(ps);
			System.out.println("executeCommand returned: " + (resp == null ? "NULL" : resp.getClass().getSimpleName()));
			if (resp instanceof PayloadStruct) {
				PayloadStruct back = (PayloadStruct) resp;
				System.out.println("  operation=" + back.operation + " response=" + back.response);
				System.out.println("  payload=" + java.util.Arrays.toString(back.payload));
			}
		} catch (Exception e) {
			System.out.println("executeCommand FAILED: " + e.getClass().getSimpleName() + ": " + e.getMessage());
		}

		try {
			client.stopServer();
			client.close();
		} catch (Exception ignored) {
			// teardown is best-effort; the round trip above is the point of this harness
		}
		t.close();
		System.out.println("done");
	}
}
