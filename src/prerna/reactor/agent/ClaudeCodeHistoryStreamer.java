package prerna.reactor.agent;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.function.Function;
import java.nio.file.*;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import prerna.util.Utility;
import prerna.websocket.FileStreamer;
import prerna.websocket.SocketSessionHandler;
import prerna.websocket.SocketSessionHandlerFactory;
import prerna.reactor.agent.ClaudeCodeTranscriptModels;

public class ClaudeCodeHistoryStreamer implements FileStreamer {
	
	private static final Logger logger = LogManager.getLogger(ClaudeCodeHistoryStreamer.class);
		
	private final Path filePath;
	private final String insightId;
	private final Function<JSONObject, JSONObject> transform;
	private volatile boolean running = false;
	
	/**
	 * @param filePath   absolute path to the JSONL file to tail
	 * @param insightId  the insightId whose WS clients should receive updates
	 * @param transform  a function that reshapes each raw JSON line before it is
	 *                   sent to the client; return {@code null} to skip a line
	 */
	public ClaudeCodeHistoryStreamer(String roomId, String insightId,
			Function<JSONObject, JSONObject> transform) {
		
		String filePath = getRoomPath(roomId);
		
		this.filePath = Path.of(filePath);
		this.insightId = insightId;
		this.transform = transform;
	}
	
	public String getRoomPath(String roomId) {
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
	            .map(Path::toString)
	            .findFirst()
	            .orElse(null);
	    } catch (IOException e) {
	        return null;
	    }
	}
	
	/**
	 * Begin tailing the file. Blocks the calling thread until {@link #stop()}
	 * is called or the thread is interrupted.
	 */
	public void start() {
		if (!Files.exists(filePath)) {
			logger.error("JSONL file does not exist: {}", filePath);
			return;
		}

		running = true;
		Path dir = filePath.getParent();
		Path fileName = filePath.getFileName();

		try (WatchService watcher = FileSystems.getDefault().newWatchService();
				RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r")) {

			dir.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);

			// Seek to end — only process lines written after we start
			raf.seek(raf.length());
			logger.info("Tailing {} for insightId={}", filePath, insightId);

			String partialLine = "";

			while (running) {
				WatchKey key;
				try {
					key = watcher.take(); // blocks until a change occurs
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}

				for (WatchEvent<?> event : key.pollEvents()) {
					Path changed = (Path) event.context();
					if (!fileName.equals(changed)) {
						continue;
					}

					// Read all new content from where we left off
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

				if (!key.reset()) {
					logger.warn("Watch key no longer valid for {}", dir);
					break;
				}
			}
		} catch (IOException e) {
			logger.error("Error tailing JSONL file {}", filePath, e);
		}

		logger.info("Stopped tailing {}", filePath);
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
