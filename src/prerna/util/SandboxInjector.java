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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Client for the namespace sandbox supervisor's control socket (see
 * py/sandbox_launcher.py).
 */
public class SandboxInjector {

	private static final Logger classLogger = LogManager.getLogger(SandboxInjector.class);

	private final String controlSocketPath;

	public SandboxInjector(String controlSocketPath) {
		this.controlSocketPath = controlSocketPath;
	}

	public boolean inject(String absPath, boolean readWrite) {
		return send("INJECT\t" + (readWrite ? "rw" : "ro") + "\t" + absPath);
	}

	public boolean remove(String absPath) {
		return send("REMOVE\t" + absPath);
	}

	public boolean ping() {
		return trySend("PING", false);
	}

	public boolean awaitReady(long timeoutMillis) {
		long deadline = System.currentTimeMillis() + timeoutMillis;
		while (System.currentTimeMillis() < deadline) {
			if (ping()) {
				return true;
			}
			try {
				Thread.sleep(100L);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		return false;
	}

	public String getControlSocketPath() {
		return controlSocketPath;
	}

	private synchronized boolean send(String command) {
		return trySend(command, true);
	}

	private boolean trySend(String command, boolean required) {
		if (controlSocketPath == null || controlSocketPath.isEmpty()) {
			if (required) {
				throw new IllegalStateException(
						"No sandbox control socket configured for " + command.replace('\t', ' '));
			}
			return false;
		}
		UnixDomainSocketAddress address = UnixDomainSocketAddress.of(controlSocketPath);
		try (SocketChannel ch = SocketChannel.open(address)) {
			OutputStream os = Channels.newOutputStream(ch);
			os.write((command + "\n").getBytes(StandardCharsets.UTF_8));
			os.flush();

			InputStream is = Channels.newInputStream(ch);
			StringBuilder sb = new StringBuilder();
			int c;
			while ((c = is.read()) != -1 && c != '\n') {
				sb.append((char) c);
			}
			String reply = sb.toString().trim();
			if (reply.startsWith("OK") || "PONG".equals(reply)) {
				return true;
			}
			String msg = "Sandbox control command '" + command.replace('\t', ' ') + "' failed: " + reply;
			if (required) {
				throw new IllegalStateException(msg);
			}
			classLogger.debug(msg);
			return false;
		} catch (IOException e) {
			if (required) {
				throw new IllegalStateException("Failed to talk to sandbox control socket " + controlSocketPath, e);
			}
			classLogger.debug("Sandbox control socket {} is not ready", controlSocketPath, e);
			return false;
		}
	}
}
