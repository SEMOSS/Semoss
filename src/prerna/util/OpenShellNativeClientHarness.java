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

import prerna.tcp.client.NativePySocketClient;

/**
 * Drives the REAL client ({@link NativePySocketClient}) against a sandbox created by
 * {@link OpenShellTransport}.
 *
 * <p>
 * This exists because the hand-rolled {@link OpenShellTransportHarness} proved the
 * transport and then became the blocker: its own read path kept failing while an
 * independent client succeeded against the same sandbox. Debugging through test
 * scaffolding is how you spend hours on the wrong layer. This harness has no protocol
 * code of its own -- it only wires the transport to the class that will actually ship.
 *
 * <p>
 * Usage: {@code java ... prerna.util.OpenShellNativeClientHarness <sessionKey>}
 */
public final class OpenShellNativeClientHarness {

	public static void main(String[] args) throws Exception {
		String rdf = System.getProperty("RDF_MAP", "/opt/semosshome/RDF_Map.prop");
		DIHelper.getInstance().loadCoreProp(rdf);
		String sessionKey = args.length > 0 ? args[0] : "native-client-1";

		OpenShellTransport t = OpenShellTransport.forSession(sessionKey);
		int port = t.start();
		System.out.println("transport up on port " + port);

		// Wait for the worker WITHOUT touching its port: connecting to probe would
		// consume the single connection slot and kill the worker.
		System.out.println("settling 4s...");
		Thread.sleep(4000);

		NativePySocketClient client = new NativePySocketClient(java.util.Map.of());
		client.connect("127.0.0.1", port, false);
		System.out.println("NativePySocketClient connected");

		Thread reader = new Thread(client, "native-client-reader");
		reader.setDaemon(true);
		reader.start();
		client.awaitReadyOrKill(30_000, 500);
		System.out.println("client ready=" + client.isReady() + " connected=" + client.isConnected());

		client.stopServer();
		client.close();
		t.close();
		System.out.println("done");
	}
}
