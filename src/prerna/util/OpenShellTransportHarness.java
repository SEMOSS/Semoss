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

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Standalone harness for {@link OpenShellTransport}, run outside Tomcat.
 *
 * <p>
 * Exists because debugging this inside a servlet was guesswork: the failure
 * surfaced as "Received a null PayloadStruct response" several layers away from
 * the cause, and the transport's own INFO lines were not visible in the Tomcat
 * log. Running the same code with a console attached removes both problems.
 *
 * <p>
 * Usage: {@code java ... prerna.util.OpenShellTransportHarness <sessionKey>}
 */
public final class OpenShellTransportHarness {

	private static String canConnect(int port) {
		try (Socket s = new Socket()) {
			s.connect(new InetSocketAddress("127.0.0.1", port), 2000);
			return "yes";
		} catch (Exception e) {
			return "no (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")";
		}
	}

	private static byte[] readExactly(java.io.InputStream in, int n) throws java.io.IOException {
		byte[] buf = new byte[n];
		int off = 0;
		while (off < n) {
			int read = in.read(buf, off, n - off);
			if (read < 0) {
				throw new java.io.IOException("peer closed after " + off + "/" + n + " bytes");
			}
			off += read;
		}
		return buf;
	}

	public static void main(String[] args) throws Exception {
		// Bootstrap the DIHelper from the RDF map exactly as the webapp does. Without
		// this, Utility.getDIHelperProperty() returns null for every property -- which
		// is how an earlier round of debugging lost three iterations to a sandbox that
		// was created from the stock image because OPENSHELL_IMAGE "was not set".
		String rdf = System.getProperty("RDF_MAP");
		if (rdf == null || rdf.trim().isEmpty()) {
			rdf = "/opt/semosshome/RDF_Map.prop";
		}
		DIHelper.getInstance().loadCoreProp(rdf);
		System.out.println("loaded RDF: " + rdf);
		System.out.println("OPENSHELL_IMAGE=" + Utility.getDIHelperProperty(OpenShellTransport.IMAGE_PROP));
		System.out.println("OPENSHELL_POLICY=" + Utility.getDIHelperProperty(OpenShellTransport.POLICY_PROP));

		String sessionKey = args.length > 0 ? args[0] : "harness-session-1";
		System.out.println("sandbox name would be: " + OpenShellTransport.sandboxNameFor(sessionKey));
		System.out.println("name length: " + OpenShellTransport.sandboxNameFor(sessionKey).length() + " (max "
				+ OpenShellTransport.MAX_SANDBOX_NAME_LENGTH + ")");

		System.out.println("EXPECTED ARGV:");
		System.out.println("  " + OpenShellTransport.describeCreateCommand(sessionKey));
		OpenShellTransport t = OpenShellTransport.forSession(sessionKey);
		System.out.println("starting transport...");
		long t0 = System.currentTimeMillis();
		int port = t.start();
		System.out.println("start() returned port " + port + " after " + (System.currentTimeMillis() - t0) + "ms");
		System.out.println("is that port actually LISTENING?  "
				+ canConnect(port));

		// Wait for the worker WITHOUT touching its port. "Forwarding" is printed when
		// the CLI's listener binds, which precedes the worker's own readiness. A sleep
		// is deliberate: probing the port would consume the worker's single connection
		// slot and its disconnect would kill the worker -- see the wiki rule.
		System.out.println("settling 4s before the single connection...");
		Thread.sleep(4000);
		try (Socket s = new Socket()) {
			s.connect(new InetSocketAddress("127.0.0.1", port), 5000);
			// One framed request, exactly as the prototype does: [4-byte size][20-byte epoch][payload]
			byte[] payload = ("{\"epoc\":\"00000000000000000001\",\"operation\":\"PYTHON\",\"methodName\":\"probe\","
					+ "\"payload\":[\"import os; print('HARNESS_OK', os.getpid())\"],\"payloadClasses\":"
					+ "[\"java.lang.String\"],\"longRunning\":true,\"hasReturn\":true,\"response\":false,"
					+ "\"interim\":false,\"insightId\":\"harness\",\"executionInsightId\":\"harness\","
					+ "\"sessionId\":\"harness\",\"jobId\":\"harness\",\"mdc\":{},\"asset_paths\":[],"
					+ "\"runtime_vars\":{},\"append_vars\":{}}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
			byte[] epoc = "00000000000000000001".getBytes("UTF-8");
			java.io.OutputStream out = s.getOutputStream();
			out.write(new byte[] { (byte) (payload.length >>> 24), (byte) (payload.length >>> 16),
					(byte) (payload.length >>> 8), (byte) payload.length });
			out.write(epoc);
			out.write(payload);
			out.flush();

			java.io.InputStream in = s.getInputStream();
			// readNBytes() may return fewer bytes than asked on a stream (and throws on
			// an empty read if the peer closed). Read exactly N, or fail with the count.
			byte[] hdr = readExactly(in, 4);
			int size = ((hdr[0] & 0xff) << 24) | ((hdr[1] & 0xff) << 16) | ((hdr[2] & 0xff) << 8) | (hdr[3] & 0xff);
			System.out.println("reply declared size=" + size);
			byte[] body = readExactly(in, size);
			System.out.println("reply body=" + new String(body, java.nio.charset.StandardCharsets.UTF_8));
			// Keep reading: the worker streams LOG frames before the terminal response.
			for (int i = 0; i < 5; i++) {
				try {
					byte[] h2 = readExactly(in, 4);
					int s2 = ((h2[0] & 0xff) << 24) | ((h2[1] & 0xff) << 16) | ((h2[2] & 0xff) << 8) | (h2[3] & 0xff);
					byte[] b2 = readExactly(in, s2);
					System.out.println("frame " + (i + 2) + " (" + s2 + " bytes): "
							+ new String(b2, java.nio.charset.StandardCharsets.UTF_8));
				} catch (Exception more) {
					System.out.println("(no more frames: " + more.getMessage() + ")");
					break;
				}
			}
		}

		System.out.println("closing transport...");
		t.close();
		System.out.println("done");
	}
}
