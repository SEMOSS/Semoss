package prerna.reactor.agent;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.util.Utility;
import prerna.websocket.FileStreamer;
import prerna.websocket.SocketSessionHandler;
import prerna.websocket.SocketSessionHandlerFactory;

public class ClaudeCodeHistoryStreamer implements FileStreamer {

	private static final Logger logger = LogManager.getLogger(ClaudeCodeHistoryStreamer.class);

	/** How often to poll for the file/directory to appear (ms) */
	private static final long POLL_INTERVAL_MS = 2000;

	private final String roomId;
	private final String insightId;
	private final Function<JSONObject, JSONObject> transform;
	private volatile boolean running = false;

	/**
	 * @param roomId     the room whose JSONL transcript to tail
	 * @param insightId  the insightId whose WS clients should receive updates
	 * @param transform  a function that reshapes each raw JSON line before it is
	 *                   sent to the client; return {@code null} to skip a line
	 */
	public ClaudeCodeHistoryStreamer(String roomId, String insightId,
			Function<JSONObject, JSONObject> transform) {
		this.roomId = roomId;
		this.insightId = insightId;
		this.transform = transform;
	}

	/**
	 * Search the room folder for the JSONL file.
	 * Returns null if the room dir or file doesn't exist yet.
	 */
	private Path findJsonlFile() {
	    String roomFolderPath = Utility.getBaseFolder() + File.separator + "room" + File.separator + roomId;
	    Path rootDir = Paths.get(roomFolderPath);

	    if (!Files.isDirectory(rootDir)) {
	        return null;
	    }

	    String targetFileName = roomId + ".jsonl";

	    try (Stream<Path> walk = Files.walk(rootDir)) {
	        return walk
	            .filter(Files::isRegularFile)
	            .filter(p -> p.getFileName().toString().equals(targetFileName))
	            .findFirst()
	            .orElse(null);
	    } catch (IOException e) {
	        return null;
	    }
	}

	/**
	 * Begin tailing the file. If the file doesn't exist yet, polls until it
	 * appears (or until {@link #stop()} is called). Blocks the calling thread.
	 */
	public void start() {
		running = true;

		// Phase 1: Wait for the file to appear
		Path resolvedPath = findJsonlFile();
		if (resolvedPath == null) {
			logger.info("JSONL file for room {} does not exist yet — waiting for it to appear", roomId);
			resolvedPath = waitForFile();
		}

		if (resolvedPath == null) {
			// stop() was called while we were waiting
			logger.info("Stopped waiting for JSONL file (room={})", roomId);
			return;
		}

		// Phase 2: Tail the file
		tailFile(resolvedPath);

		logger.info("Stopped tailing room={}", roomId);
	}

	/**
	 * Poll until the JSONL file appears on disk.
	 * Returns null if stop() is called before the file is found.
	 */
	private Path waitForFile() {
		while (running) {
			try {
				Thread.sleep(POLL_INTERVAL_MS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return null;
			}

			Path found = findJsonlFile();
			if (found != null) {
				logger.info("JSONL file appeared: {}", found);
				return found;
			}
		}
		return null;
	}

	/**
	 * Tail the JSONL file, reading new lines as they are appended.
	 *
	 * Uses poll(timeout) instead of take() so we always attempt a read
	 * on each cycle. This avoids a race condition on macOS where the
	 * WatchService (which is poll-based, not native) can miss events
	 * that happen before its internal baseline scan completes.
	 */
	private void tailFile(Path filePath) {
		Path dir = filePath.getParent();

		try (WatchService watcher = FileSystems.getDefault().newWatchService();
				RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {

			dir.register(watcher,
					StandardWatchEventKinds.ENTRY_MODIFY,
					StandardWatchEventKinds.ENTRY_CREATE);

			// Seek to end — only process lines written after we start
			raf.seek(raf.length());
			logger.info("Tailing {} for insightId={}", filePath, insightId);

			String partialLine = "";

			while (running) {
				// Poll with a short timeout instead of blocking indefinitely.
				// This ensures we always try a read even if the OS-level
				// watcher missed the event (common on macOS).
				WatchKey key;
				try {
					key = watcher.poll(POLL_INTERVAL_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}

				// Drain events if any (we don't need to inspect them —
				// we always try to read new data regardless)
				if (key != null) {
					key.pollEvents();
					if (!key.reset()) {
						logger.warn("Watch key no longer valid for {}", dir);
						break;
					}
				}

				// Always attempt to read new lines, whether or not a
				// WatchEvent fired. This is the key fix — catches data
				// written during the race window after file discovery.
				String line;
				while ((line = raf.readLine()) != null) {
					line = partialLine + line;
					partialLine = "";

					line = line.trim();
					if (line.isEmpty()) {
						continue;
					}

					processLine(line);
				}
			}
		} catch (IOException e) {
			logger.error("Error tailing JSONL file {}", filePath, e);
		}
	}

	/**
	 * Parse a single JSONL line, apply the transform, and broadcast to WS clients.
	 */
	private void processLine(String line) {
		try {
			JSONObject raw = new JSONObject(line);
			JSONObject transformed = transform.apply(raw);

			if (transformed == null) {
				// transform returned null — skip this line
				return;
			}

			SocketSessionHandler handler = SocketSessionHandlerFactory.getHandler(insightId);
			handler.updateRecipe(transformed.toString());
		} catch (Exception e) {
			logger.warn("Failed to process JSONL line: {}", line, e);
		}
	}

	/** Signal the tailer to stop after the current poll cycle. */
	public void stop() {
		running = false;
	}

	/** Whether the tailer is currently running. */
	public boolean isRunning() {
		return running;
	}
}
