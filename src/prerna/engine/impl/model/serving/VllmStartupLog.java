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
package prerna.engine.impl.model.serving;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.util.k8s.KubeJson;

/**
 * The milestones vLLM logs while it starts, read from the model container's log
 * (requested with {@code timestamps=true}, so each line starts with its RFC 3339
 * time). The marker strings are vLLM 0.11's (NGC 25.11); if a new image logs
 * differently, the phases degrade to "loading" until the container turns ready,
 * which is why they are hints and never the source of READY.
 */
public final class VllmStartupLog {

	public enum Milestone {
		ARCHITECTURE_RESOLVED("Resolved architecture:"),
		LOADING_STARTED("Starting to load model"),
		WEIGHTS_LOADED("Loading weights took"),
		COMPILED("torch.compile takes"),
		KV_CACHE_SIZED("Available KV cache memory:"),
		GRAPHS_CAPTURED("Graph capturing finished"),
		ENGINE_READY("init engine (profile"),
		SERVER_STARTING("Starting vLLM API server"),
		STARTUP_COMPLETE("Application startup complete");

		private final String marker;

		Milestone(String marker) {
			this.marker = marker;
		}
	}

	/** torch.compile's output came from vLLM's cache (VLLM_CACHE_ROOT), as on a restart */
	private static final String COMPILE_CACHE_HIT = "Directly load the compiled graph";
	/** vLLM compiled the model afresh, and keeps the result for the next start */
	private static final String COMPILE_CACHE_MISS = "Compiling a graph for";
	private static final Pattern ANSI_ESCAPE = Pattern.compile("\u001B\\[[0-9;?]*[ -/]*[@-~]");
	/** uvicorn access lines for probes and scrapes: a 10 s heartbeat that buries everything else */
	private static final Pattern ACCESS_LOG = Pattern
			.compile("\"(GET|HEAD) /(health|metrics|ping|version|v1/models)\\b[^\"]*\" \\d{3}");
	private static final Pattern ARCHITECTURE = Pattern.compile("Resolved architecture: (\\S+)");
	private static final Pattern MAX_MODEL_LEN = Pattern.compile("Using max model len (\\d+)");
	private static final Pattern WEIGHTS = Pattern.compile("Model loading took ([0-9.]+) GiB");
	private static final Pattern KV_CACHE = Pattern.compile("Available KV cache memory: ([0-9.]+) GiB");
	private static final Pattern ERROR_LINE = Pattern.compile("\\b[A-Za-z]*(Error|Exception)\\b");
	/** Python logging as KServe's storage-initializer writes it: "2026-10-27 19:07:00.775 1 storage.initializer ERROR [file.py:func():42] " */
	private static final Pattern PYTHON_LOG_PREFIX = Pattern
			.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(?:[.,]\\d+)? \\d+ \\S+ [A-Z]+ \\[[^\\]]*\\] ");
	/** vLLM's process tag and level: "(EngineCore_DP0 pid=129) ERROR 10-26 23:58:59 [core.py:42] " */
	private static final Pattern VLLM_LOG_PREFIX = Pattern.compile(
			"^(?:\\([^)]*pid=\\d+\\) )?(?:(?:DEBUG|INFO|WARNING|ERROR|CRITICAL) \\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} \\[[^\\]]*\\] )?");
	/** Hugging Face appends a request id to its errors */
	private static final Pattern REQUEST_ID = Pattern.compile("\\s*\\(Request ID: [^)]*\\)");

	private final Map<Milestone, Instant> reached = new EnumMap<>(Milestone.class);
	private String architecture;
	private Integer maxModelLen;
	private Double weightsGiB;
	private Double kvCacheGiB;
	private boolean compileCacheHit;
	private boolean compileCacheMiss;

	private VllmStartupLog() {
	}

	public static VllmStartupLog parse(List<String> lines) {
		VllmStartupLog log = new VllmStartupLog();
		if (lines == null) {
			return log;
		}
		for (String raw : lines) {
			String line = clean(raw);
			for (Milestone milestone : Milestone.values()) {
				if (!log.reached.containsKey(milestone) && line.contains(milestone.marker)) {
					log.reached.put(milestone, timestampOf(line));
				}
			}
			if (log.architecture == null) {
				log.architecture = group(ARCHITECTURE, line);
			}
			if (log.maxModelLen == null) {
				String value = group(MAX_MODEL_LEN, line);
				log.maxModelLen = value == null ? null : Integer.valueOf(value);
			}
			if (log.weightsGiB == null) {
				String value = group(WEIGHTS, line);
				log.weightsGiB = value == null ? null : Double.valueOf(value);
			}
			if (log.kvCacheGiB == null) {
				String value = group(KV_CACHE, line);
				log.kvCacheGiB = value == null ? null : Double.valueOf(value);
			}
			log.compileCacheHit |= line.contains(COMPILE_CACHE_HIT);
			log.compileCacheMiss |= line.contains(COMPILE_CACHE_MISS);
		}
		return log;
	}

	private static String group(Pattern pattern, String line) {
		Matcher matcher = pattern.matcher(line);
		return matcher.find() ? matcher.group(1) : null;
	}

	public boolean hasReached(Milestone milestone) {
		return this.reached.containsKey(milestone);
	}

	/** @return when the milestone was logged, or null if not reached or untimed */
	public Instant timeOf(Milestone milestone) {
		return this.reached.get(milestone);
	}

	public String getArchitecture() {
		return this.architecture;
	}

	public Integer getMaxModelLen() {
		return this.maxModelLen;
	}

	public Double getWeightsGiB() {
		return this.weightsGiB;
	}

	public Double getKvCacheGiB() {
		return this.kvCacheGiB;
	}

	/**
	 * @return "hit" when vLLM loaded the compiled model from its cache, "miss" when
	 *         it compiled it, "partial" for both, and null before it compiles
	 */
	public String getCompileCache() {
		if (this.compileCacheHit) {
			return this.compileCacheMiss ? "partial" : "hit";
		}
		return this.compileCacheMiss ? "miss" : null;
	}

	/** Remove terminal color codes (vLLM colors its process prefixes). */
	public static String clean(String line) {
		return line == null ? "" : ANSI_ESCAPE.matcher(line).replaceAll("");
	}

	public static boolean isAccessLog(String line) {
		return ACCESS_LOG.matcher(line).find();
	}

	/** @return the leading RFC 3339 timestamp of a {@code timestamps=true} log line, or null */
	public static Instant timestampOf(String line) {
		int space = line.indexOf(' ');
		return space <= 0 ? null : KubeJson.parseTime(line.substring(0, space));
	}

	/** @return the line without its leading timestamp, if it has one */
	public static String withoutTimestamp(String line) {
		int space = line.indexOf(' ');
		if (space > 0 && KubeJson.parseTime(line.substring(0, space)) != null) {
			return line.substring(space + 1);
		}
		return line;
	}

	/**
	 * Split a log body into cleaned lines.
	 *
	 * @param includeAccessLogs keep the probe and scrape access lines
	 */
	public static List<String> lines(String body, boolean includeAccessLogs) {
		if (body == null || body.isEmpty()) {
			return Collections.emptyList();
		}
		List<String> lines = new ArrayList<>();
		for (String raw : body.split("\\r?\\n")) {
			String line = clean(raw);
			if (line.isBlank() || (!includeAccessLogs && isAccessLog(line))) {
				continue;
			}
			lines.add(line);
		}
		return lines;
	}

	/**
	 * The most telling line of a crashed process's log: Python prints the
	 * exception last, so the last line naming an error or exception, else the last
	 * line.
	 *
	 * @return the line without its timestamp, or null for an empty log
	 */
	public static String mostTellingLine(List<String> lines) {
		if (lines == null || lines.isEmpty()) {
			return null;
		}
		for (int index = lines.size() - 1; index >= 0; index--) {
			String line = message(lines.get(index));
			if (ERROR_LINE.matcher(line).find()) {
				return line;
			}
		}
		return message(lines.get(lines.size() - 1));
	}

	/**
	 * What a log line says, without kubectl's timestamp, colours, the program's
	 * own log prefix (Python logging, vLLM's process tag and level) and Hugging
	 * Face's request id.
	 */
	static String message(String line) {
		String text = withoutTimestamp(clean(line)).strip();
		text = PYTHON_LOG_PREFIX.matcher(text).replaceFirst("");
		text = VLLM_LOG_PREFIX.matcher(text).replaceFirst("");
		return REQUEST_ID.matcher(text).replaceAll("").strip();
	}
}
