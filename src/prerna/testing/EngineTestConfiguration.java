package prerna.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.AssetUtility;
import prerna.util.EngineUtility;
import prerna.util.SystemDefaultEngines;
import prerna.util.Utility;

/** App-owned database substitutions. Saving never changes the published application. */
public final class EngineTestConfiguration {
	private static final Gson JSON = new Gson();
	private EngineTestConfiguration() { }

	public static Path file(String projectId) {
		return Path.of(AssetUtility.getProjectAppRootFolder(projectId), "app-tests", "engine-test-configuration.json");
	}

	/** Read a profile; absence is distinct from corrupt or inaccessible configuration. */
	public static synchronized Map<String, Object> load(String projectId) {
		return read(file(projectId));
	}

	static Map<String, Object> read(Path file) {
		if (Files.notExists(file)) return null;
		try {
			if (Files.size(file) > 32_000) throw new IllegalArgumentException("Engine test configuration exceeds its size limit");
			Map<String, Object> value = JSON.fromJson(Files.readString(file), new TypeToken<Map<String, Object>>() { }.getType());
			return normalize(value);
		} catch (IOException | RuntimeException e) {
			throw new IllegalArgumentException("Unable to read engine test configuration; no production fallback is allowed", e);
		}
	}

	/** Require canonical, non-chained database mappings and explicit profile identity. */
	static Map<String, Object> normalize(Map<String, Object> input) {
		if (input == null || !(input.get("mappings") instanceof List<?> entries) || entries.isEmpty() || entries.size() > 16)
			throw new IllegalArgumentException("Provide between 1 and 16 database mappings");
		if (input.containsKey("version") && (!(input.get("version") instanceof Number n) || n.intValue() != 1 || n.doubleValue() != 1))
			throw new IllegalArgumentException("Unsupported engine test configuration version");
		Map<String, String> mappings = new LinkedHashMap<>();
		for (Object entry : entries) {
			if (!(entry instanceof Map<?, ?> pair)) throw new IllegalArgumentException("Invalid engine mapping");
			String real = engineId(pair.get("realEngineId")), test = engineId(pair.get("testEngineId"));
			if (real.equals(test) || mappings.putIfAbsent(real, test) != null)
				throw new IllegalArgumentException("Real and test IDs must differ and each real database may appear only once");
		}
		if (mappings.values().stream().anyMatch(mappings::containsKey))
			throw new IllegalArgumentException("Test databases cannot also be mapped production databases");
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("version", 1);
		out.put("id", input.containsKey("id") ? UUID.fromString(String.valueOf(input.get("id"))).toString() : UUID.randomUUID().toString());
		out.put("name", text(input.getOrDefault("name", "Test databases"), 120));
		out.put("mappings", mappings.entrySet().stream().map(e -> Map.of("realEngineId", e.getKey(), "testEngineId", e.getValue())).toList());
		out.put("denyUnmappedDatabases", true);
		out.put("updatedAt", input.getOrDefault("updatedAt", Instant.now().toString()));
		return out;
	}

	private static String text(Object value, int maximum) {
		if (!(value instanceof String s) || s.isBlank() || s.length() > maximum) throw new IllegalArgumentException("Invalid engine test configuration name");
		return s.trim();
	}
	private static String engineId(Object value) {
		String id = text(value, 128);
		if (!id.matches("[A-Za-z0-9_-]+") || SystemDefaultEngines.getIgnoreDatabaseOwlList().contains(id))
			throw new IllegalArgumentException("Only application database IDs may be mapped");
		return id;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> mappings(Map<String, Object> configuration) {
		Map<String, String> result = new LinkedHashMap<>();
		for (Map<String, String> pair : (List<Map<String, String>>) configuration.get("mappings"))
			result.put(pair.get("realEngineId"), pair.get("testEngineId"));
		return Map.copyOf(result);
	}

	/** Validate permissions and physical separation on every activation, not just save. */
	public static void authorize(User user, Map<String, Object> configuration) {
		for (var pair : mappings(configuration).entrySet()) {
			String real = pair.getKey(), test = pair.getValue();
			if (!SecurityEngineUtils.userCanViewEngine(user, real) || !SecurityEngineUtils.userCanEditEngine(user, test))
				throw new IllegalArgumentException("Viewing the real database and editing the test database are required");
			if (SecurityEngineUtils.getEngineTypeAndSubtype(real)[0] != IEngine.CATALOG_TYPE.DATABASE
					|| SecurityEngineUtils.getEngineTypeAndSubtype(test)[0] != IEngine.CATALOG_TYPE.DATABASE)
				throw new IllegalArgumentException("Engine test mappings must reference database engines");
			if (!(Utility.getDatabase(test) instanceof IRDBMSEngine target))
				throw new IllegalArgumentException("Use a dedicated local H2 or SQLite test database");
			// A cloned external engine can point at production. Accept only a file within the test engine's own folder.
			Path targetFile = storageFile(target.getConnectionUrl());
			try {
				Path engineFolder = Path.of(EngineUtility.getSpecificEngineBaseFolder(test)).toRealPath();
				if (!targetFile.getParent().toRealPath().startsWith(engineFolder)) throw new IOException("Test file outside engine folder");
				if (Files.isSymbolicLink(targetFile) || Files.exists(targetFile) && (!targetFile.toRealPath().startsWith(engineFolder) || hasMultipleLinks(targetFile)))
					throw new IOException("Test file points outside engine folder");
			} catch (IOException e) {
				throw new IllegalArgumentException("Test database storage must belong to its own engine folder", e);
			}
			if (Utility.getDatabase(real) instanceof IRDBMSEngine source && source.getConnectionUrl() != null) {
                Path sourceFile = null;
                try { sourceFile = storageFile(source.getConnectionUrl()); } catch (IllegalArgumentException unsupportedSource) { /* Production may be remote. */ }
                try {
                    if (source.getConnectionUrl().equals(target.getConnectionUrl()) || sourceFile != null
                            && (sourceFile.equals(targetFile) || Files.exists(sourceFile) && Files.exists(targetFile) && Files.isSameFile(sourceFile, targetFile)))
                        throw new IllegalArgumentException("Real and test engines point at the same physical database");
                } catch (IOException e) { throw new IllegalArgumentException("Unable to verify database storage separation", e); }
            }
		}
	}

    private static boolean hasMultipleLinks(Path file) throws IOException {
        if (!file.getFileSystem().supportedFileAttributeViews().contains("unix")) return false;
        return ((Number) Files.getAttribute(file, "unix:nlink")).longValue() > 1;
    }
    /** H2's JDBC path omits the suffix of its actual storage file. */
    static Path storageFile(String url) {
        Path base = localFile(url);
        return url.startsWith("jdbc:h2:") ? Path.of(base + ".mv.db") : base;
    }

	static Path localFile(String url) {
		if (url == null) throw new IllegalArgumentException("Test database has no connection URL");
		String value;
		if (url.startsWith("jdbc:sqlite:")) value = url.substring("jdbc:sqlite:".length());
		else if (url.startsWith("jdbc:h2:nio:")) value = url.substring("jdbc:h2:nio:".length());
		else if (url.startsWith("jdbc:h2:file:")) value = url.substring("jdbc:h2:file:".length());
		else throw new IllegalArgumentException("Use a dedicated file-backed H2 or SQLite test database; remote engine clones are not isolated");
		if (value.contains(";")) {
			String[] parts = value.split(";");
			for (int i = 1; i < parts.length; i++)
				if (!parts[i].matches("(?i)(query_timeout|cache_size)=[0-9]+"))
					throw new IllegalArgumentException("Unsupported test database connection option");
			value = parts[0];
		}
		if (value.contains("?") || !Path.of(value).isAbsolute())
			throw new IllegalArgumentException("Test database URL must be a plain absolute file path without connection options");
		return Path.of(value).normalize();
	}

	public static synchronized Map<String, Object> save(User user, String projectId, Map<String, Object> input) {
		Map<String, Object> out = normalize(input);
		authorize(user, out);
		out.put("updatedAt", Instant.now().toString());
		write(file(projectId), out);
		return out;
	}

	static void write(Path file, Object value) {
		try {
			Files.createDirectories(file.getParent());
			Path temporary = Files.createTempFile(file.getParent(), "engine-test-", ".tmp");
			try {
				Files.writeString(temporary, JSON.toJson(value));
				try { Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
				catch (java.nio.file.AtomicMoveNotSupportedException e) { Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING); }
			} finally { Files.deleteIfExists(temporary); }
		} catch (IOException e) { throw new IllegalArgumentException("Unable to persist engine test configuration", e); }
	}
}
