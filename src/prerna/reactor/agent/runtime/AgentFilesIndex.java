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
package prerna.reactor.agent.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * What each of an agent's own files holds, kept beside them as
 * {@value #FILE_NAME}: one line of description for a file, so a run is told
 * what a file is about and not only its name. A missing or unreadable index is
 * an empty one, and a description counts only while its file still has the size
 * it was written for.
 *
 * <pre>
 * { "version": 1, "files": { "emails/Kickoff.md": { "about": "...", "size": 1234 } } }
 * </pre>
 */
public final class AgentFilesIndex {

	private static final Logger classLogger = LogManager.getLogger(AgentFilesIndex.class);

	/** The index, at the top of the agent's files folder. */
	public static final String FILE_NAME = "agent-files-index.json";
	/** The most a description holds; longer text is cut when read as well as when written. */
	public static final int MAX_ABOUT = 200;
	// an index of thousands of files is still well under this; anything larger is not ours
	private static final long MAX_BYTES = 1024L * 1024L;
	private static final int VERSION = 1;

	/** A file's description, and the size in bytes the file had when it was written. */
	public record Entry(String about, long size) {
	}

	private AgentFilesIndex() {
	}

	/**
	 * The files of a folder of agent files that a run is told about, by path inside
	 * the folder with forward slashes and in path order, each with its size in
	 * bytes. Names starting with a dot are bookkeeping, as are this index and the
	 * placeholder.txt the file explorer puts into every folder it creates, and a
	 * link can point anywhere, so none of these is listed.
	 */
	public static Map<String, Long> listFiles(Path agentFilesDir) throws IOException {
		Map<String, Long> files = new TreeMap<>();
		try (Stream<Path> walk = Files.walk(agentFilesDir)) {
			Iterator<Path> paths = walk.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).iterator();
			while (paths.hasNext()) {
				Path path = paths.next();
				String file = agentFilesDir.relativize(path).toString().replace('\\', '/');
				if (!file.startsWith(".") && !file.contains("/.") && !file.equals("placeholder.txt")
						&& !file.endsWith("/placeholder.txt") && !file.equals(FILE_NAME)) {
					files.put(file, Files.size(path));
				}
			}
		} catch (UncheckedIOException e) {
			// a folder that could not be walked to its end
			throw e.getCause();
		}
		return files;
	}

	/**
	 * The index of a folder of agent files, by each file's path inside the folder
	 * with forward slashes.
	 *
	 * @return the entries; empty when the folder has no index or it cannot be read
	 */
	public static Map<String, Entry> read(Path agentFilesDir) {
		Map<String, Entry> entries = new LinkedHashMap<>();
		if (agentFilesDir == null) {
			return entries;
		}
		Path file = agentFilesDir.resolve(FILE_NAME);
		try {
			if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) || Files.size(file) > MAX_BYTES) {
				return entries;
			}
			JSONObject files = new JSONObject(Files.readString(file, StandardCharsets.UTF_8)).optJSONObject("files");
			if (files == null) {
				return entries;
			}
			for (String path : files.keySet()) {
				JSONObject entry = files.optJSONObject(path);
				if (entry != null) {
					entries.put(path, new Entry(oneLine(entry.optString("about", "")), entry.optLong("size", -1)));
				}
			}
			return entries;
		} catch (IOException | RuntimeException e) {
			// anyone who can edit the agent's files can break this one; the run then
			// names its files without descriptions
			classLogger.warn("The agent files index in {} could not be read: {}", agentFilesDir, e.getMessage());
			return new LinkedHashMap<>();
		}
	}

	/**
	 * The index as it is saved: one file to a line, in path order, so a search of
	 * it answers with single files and a change touches only its own lines.
	 */
	public static String toJson(Map<String, Entry> entries) {
		StringBuilder json = new StringBuilder("{\n  \"version\": ").append(VERSION).append(",\n  \"files\": {");
		String separator = "\n";
		for (Map.Entry<String, Entry> file : new TreeMap<>(entries).entrySet()) {
			json.append(separator).append("    ").append(JSONObject.quote(file.getKey())).append(": { \"about\": ")
					.append(JSONObject.quote(oneLine(file.getValue().about()))).append(", \"size\": ")
					.append(file.getValue().size()).append(" }");
			separator = ",\n";
		}
		return json.append(entries.isEmpty() ? "" : "\n  ").append("}\n}\n").toString();
	}

	/**
	 * A description as one line of at most {@value #MAX_ABOUT} characters. It ends
	 * up in a prompt, so line breaks and control characters never pass.
	 */
	public static String oneLine(String about) {
		if (about == null) {
			return "";
		}
		String line = about.replaceAll("[\\p{Cntrl}\\s]+", " ").trim();
		return line.length() > MAX_ABOUT ? line.substring(0, MAX_ABOUT - 3).trim() + "..." : line;
	}
}
