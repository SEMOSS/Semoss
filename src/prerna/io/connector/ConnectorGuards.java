package prerna.io.connector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Checks for urls and local paths that are built from what a connector's remote
 * service sent back, or from an id a caller passed in.
 */
public final class ConnectorGuards {

	private static final Logger classLogger = LogManager.getLogger(ConnectorGuards.class);

	private ConnectorGuards() {
	}

	/**
	 * A link that is followed with the user's bearer token, such as an
	 * {@code @odata.nextLink}, has to stay on the service's own host.
	 *
	 * @param link the link the service returned
	 * @param base the service's base url, without a trailing slash
	 * @return the link
	 * @throws IllegalArgumentException when the link is anywhere else
	 */
	public static String requireUnder(String link, String base) {
		if (link != null) {
			try {
				URI parsed = new URI(link.trim());
				URI root = new URI(base);
				String basePath = root.getRawPath() == null ? "" : root.getRawPath();
				String linkPath = parsed.getRawPath() == null ? "" : parsed.getRawPath();
				// scheme and host compare case-insensitively, the path must be below the base path
				if (parsed.getScheme() != null && parsed.getScheme().equalsIgnoreCase(root.getScheme())
						&& parsed.getHost() != null && parsed.getHost().equalsIgnoreCase(root.getHost())
						&& effectivePort(parsed) == effectivePort(root) && parsed.getUserInfo() == null
						&& linkPath.startsWith(basePath + "/")) {
					return link;
				}
			} catch (URISyntaxException e) {
				// refused below
			}
		}
		classLogger.warn("Refused to follow a link outside {}: {}", base, hostOf(link));
		throw new IllegalArgumentException("Refused to follow a link that does not belong to " + base);
	}

	/**
	 * A pre-authenticated link, which is called without the token, has to be a plain
	 * https address.
	 *
	 * @param link the link the service returned
	 * @return the link, trimmed
	 * @throws IllegalArgumentException when it is not https or names no host
	 */
	public static String requirePreAuthenticated(String link) {
		String trimmed = link == null ? "" : link.trim();
		try {
			URI parsed = new URI(trimmed);
			if ("https".equalsIgnoreCase(parsed.getScheme()) && parsed.getHost() != null
					&& parsed.getUserInfo() == null) {
				return trimmed;
			}
		} catch (URISyntaxException e) {
			// refused below
		}
		classLogger.warn("Refused a download link that is not https: {}", hostOf(trimmed));
		throw new IllegalArgumentException("Refused a download link that is not an https address");
	}

	/**
	 * Makes an id safe to place in one segment of a url path. Ids as the services
	 * issue them, such as {@code 19:abc@thread.v2} or {@code b!01ABC}, pass through
	 * as they are; only what could change the path or the query is encoded.
	 *
	 * @param id the id
	 * @return the id, with anything that could change the path encoded
	 * @throws IllegalArgumentException when the id is blank or a dot segment
	 */
	public static String segment(String id) {
		String value = id == null ? "" : id.trim();
		if (value.isEmpty() || value.equals(".") || value.equals("..")) {
			throw new IllegalArgumentException("Not a valid id: " + id);
		}
		StringBuilder out = new StringBuilder(value.length());
		for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
			char c = (char) (b & 0xff);
			if (c < 0x80 && (Character.isLetterOrDigit(c) || "-._~!$&'()*+,;=:@".indexOf(c) >= 0)) {
				out.append(c);
			} else {
				out.append('%').append(String.format(Locale.ROOT, "%02X", b & 0xff));
			}
		}
		return out.toString();
	}

	/**
	 * Joins a relative path, as a remote listing names it, to a local folder and
	 * checks that the result is still inside that folder.
	 *
	 * @param root     the folder everything has to stay in
	 * @param relative the path below it
	 * @return the resolved path
	 * @throws IllegalArgumentException when the path would leave the folder
	 */
	public static Path resolveWithin(Path root, String relative) {
		if (relative == null || relative.isBlank()) {
			throw new IllegalArgumentException("Not a file name inside the destination: " + relative);
		}
		Path base = root.toAbsolutePath().normalize();
		try {
			// real path of the nearest existing part, so a root that is not created yet still compares true
			Path existingRoot = base;
			while (existingRoot != null && !Files.exists(existingRoot)) {
				existingRoot = existingRoot.getParent();
			}
			if (existingRoot != null) {
				base = existingRoot.toRealPath().resolve(existingRoot.relativize(base)).normalize();
			}
		} catch (IOException e) {
			throw new IllegalArgumentException("The destination cannot be resolved: " + root, e);
		}
		Path target = base.resolve(relative.replace('\\', '/')).normalize();
		if (!target.startsWith(base) || target.equals(base) || escapesThroughLink(base, target)) {
			classLogger.warn("Refused a remote file name that leaves {}", base);
			throw new IllegalArgumentException("Not a file name inside the destination: " + relative);
		}
		return target;
	}

	// a symlink below the root must not carry the write outside it
	private static boolean escapesThroughLink(Path base, Path target) {
		Path existing = target;
		while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
			existing = existing.getParent();
		}
		if (existing == null) {
			return true;
		}
		if (!existing.startsWith(base)) {
			// nothing under the root exists yet
			return false;
		}
		try {
			return !existing.toRealPath().startsWith(base);
		} catch (IOException e) {
			return true;
		}
	}

	private static int effectivePort(URI uri) {
		if (uri.getPort() >= 0) {
			return uri.getPort();
		}
		return "http".equalsIgnoreCase(uri.getScheme()) ? 80 : 443;
	}

	private static String hostOf(String link) {
		try {
			String host = link == null ? null : new URI(link.trim()).getHost();
			return host == null ? "no host" : host;
		} catch (URISyntaxException e) {
			return "an unreadable address";
		}
	}
}
