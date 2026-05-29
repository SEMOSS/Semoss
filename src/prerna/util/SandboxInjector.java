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
		return send("PING");
	}

	public String getControlSocketPath() {
		return controlSocketPath;
	}

	private synchronized boolean send(String command) {
		if (controlSocketPath == null || controlSocketPath.isEmpty()) {
			classLogger.warn("No sandbox control socket configured; cannot run '{}'",
					command.replace('\t', ' '));
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
			classLogger.warn("Sandbox control command '{}' failed: {}", command.replace('\t', ' '), reply);
			return false;
		} catch (IOException e) {
			classLogger.error("Failed to talk to sandbox control socket {}", controlSocketPath, e);
			return false;
		}
	}
}
