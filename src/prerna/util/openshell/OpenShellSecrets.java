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
package prerna.util.openshell;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.project.api.IProject;
import prerna.util.OpenShellTransport;
import prerna.util.Utility;

/**
 * Secrets an app declares in its project {@code .smss}, brokered into a user's sandbox by
 * OpenShell providers, so sandboxed code can use a secret without ever holding it.
 *
 * <pre>
 * SANDBOX_SECRETS                     [{"name":"OPENWEATHER_API_KEY","hosts":["api.openweathermap.org"]}]
 * SANDBOX_SECRET_OPENWEATHER_API_KEY  the value
 * </pre>
 *
 * <p>
 * The value stays on the host: the {@code .smss} is never uploaded into a sandbox. Each secret
 * becomes one OpenShell profile and provider, so it can only reach its own hosts. Code in the
 * sandbox gets a placeholder ({@code secret("OPENWEATHER_API_KEY")} in {@code smss_secrets}), and
 * OpenShell's supervisor swaps it for the value in requests to those hosts and refuses it
 * anywhere else. Any request method is allowed.
 *
 * <p>
 * Measured behaviour this follows (2026-10-24, OpenShell 0.1.2):
 * <ul>
 * <li>attaching a provider to a running sandbox takes tens of seconds to apply, and requests
 * in that window hang, so an attach is waited out;</li>
 * <li>an attach changes the placeholder generation and the old placeholders stop resolving, so
 * the placeholder file is rewritten from the sandbox's current environment after every attach
 * (the worker's own environment is fixed at start);</li>
 * <li>{@code provider update} also starts a new generation, within seconds, and placeholders
 * of the old one keep resolving to the <b>old</b> value; so after an update the file is
 * rewritten once the new generation appears, and an unchanged value is not updated at all.</li>
 * </ul>
 * The helper reads the file on every call, which is what lets the long-lived worker follow both.
 * The value in the property can later come from the secret store instead, which overlays the
 * same properties.
 */
public final class OpenShellSecrets {

	private static final Logger classLogger = LogManager.getLogger(OpenShellSecrets.class);

	/** The project property declaring the secrets, as a JSON list of {name, hosts}. */
	public static final String SECRETS_PROP = "SANDBOX_SECRETS";
	/** The project property holding one secret's value, followed by its name. */
	public static final String VALUE_PROP_PREFIX = "SANDBOX_SECRET_";
	/** Where the placeholders are written in the sandbox; read by {@code py/smss_secrets.py}. */
	private static final String PLACEHOLDER_PARENT = "/opt/semosshome/insight";
	private static final String PLACEHOLDER_DIR = "sandbox-secrets";
	private static final String PLACEHOLDER_FILE = "placeholders.json";

	private static final Pattern NAME = Pattern.compile("[A-Z][A-Z0-9_]{0,63}");
	/** A credential in the sandbox environment: {@code <NAME>__<project key>}. */
	private static final Pattern CREDENTIAL_ENV = Pattern.compile("([A-Z][A-Z0-9_]*)__([a-z0-9]{8})=(openshell:resolve:\\S+)");
	private static final long READY_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(90);
	private static final long READY_POLL_MS = 2000;
	private static final int CLI_TIMEOUT_S = 60;

	/** How long a new placeholder generation may take to appear after an update (measured 5–15 s). */
	private static final long GENERATION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);
	private static final Pattern GENERATION = Pattern.compile("=openshell:resolve:env:v(\\d+)_");

	private static final Map<String, Object> SANDBOX_LOCKS = new ConcurrentHashMap<>();
	/** Digest of the value last given to each provider by this JVM; unknown after a restart. */
	private static final Map<String, String> APPLIED = new ConcurrentHashMap<>();

	private OpenShellSecrets() {
		// static utility
	}

	/** One declared secret. */
	record Declared(String name, List<String> hosts) {
	}

	/**
	 * Make a project's declared secrets usable in this sandbox: register each with OpenShell,
	 * update its value, attach it if it is not yet, and refresh the placeholder file. A secret
	 * that is declared wrongly or has no value is skipped with a warning; nothing here fails
	 * the project load.
	 */
	public static void ensureForProject(OpenShellTransport transport, String projectId) {
		IProject project = Utility.getProject(projectId);
		if (project != null && project.getSmssProp() != null) {
			ensureForProject(transport, projectId, project.getSmssProp());
		}
	}

	/** {@link #ensureForProject(OpenShellTransport, String)} for a project's properties. */
	public static void ensureForProject(OpenShellTransport transport, String projectId, Properties props) {
		List<Declared> declared = parse(projectId, props.getProperty(SECRETS_PROP));
		if (declared.isEmpty()) {
			return;
		}
		String sandbox = transport.getSandboxName();
		synchronized (SANDBOX_LOCKS.computeIfAbsent(sandbox, k -> new Object())) {
			try {
				String attached = OpenShellTransport.cli(List.of("sandbox", "provider", "list", sandbox),
						CLI_TIMEOUT_S, Map.of());
				String generation = generation(sandbox);
				boolean changed = false;
				for (Declared secret : declared) {
					String value = props.getProperty(VALUE_PROP_PREFIX + secret.name());
					if (value == null || value.trim().isEmpty()) {
						classLogger.warn("Project {} declares sandbox secret {} but sets no {}{}", projectId,
								secret.name(), VALUE_PROP_PREFIX, secret.name());
						continue;
					}
					String id = providerId(projectId, secret);
					String credential = credentialName(projectId, secret.name());
					ensureProfile(id, projectId, secret, credential);
					changed |= ensureProvider(id, credential, value.trim());
					if (!containsWord(attached, id)) {
						changed = true;
						OpenShellTransport.cli(List.of("sandbox", "provider", "attach", sandbox, id), CLI_TIMEOUT_S,
								Map.of());
						awaitApplied(sandbox, id);
						classLogger.info("Attached sandbox secret {} of project {} to sandbox {}", secret.name(),
								projectId, sandbox);
					}
				}
				if (changed && generation != null) {
					awaitNewGeneration(sandbox, generation);
				}
				writePlaceholders(sandbox);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				classLogger.warn("Interrupted brokering secrets of project {} into sandbox {}", projectId, sandbox);
			} catch (Exception e) {
				classLogger.warn("Could not broker secrets of project {} into sandbox {}", projectId, sandbox, e);
			}
		}
	}

	/** The declared secrets, skipping (with a warning) any entry that is not well formed. */
	static List<Declared> parse(String projectId, String json) {
		List<Declared> declared = new ArrayList<>();
		if (json == null || json.trim().isEmpty()) {
			return declared;
		}
		JsonArray list;
		try {
			list = JsonParser.parseString(json).getAsJsonArray();
		} catch (RuntimeException e) {
			classLogger.warn("Project {} has an unreadable {}: expected a JSON list", projectId, SECRETS_PROP);
			return declared;
		}
		for (JsonElement element : list) {
			try {
				JsonObject entry = element.getAsJsonObject();
				String name = entry.get("name").getAsString().trim();
				List<String> hosts = new ArrayList<>();
				for (JsonElement host : entry.getAsJsonArray("hosts")) {
					hosts.add(host.getAsString().trim().toLowerCase());
				}
				if (!NAME.matcher(name).matches() || hosts.isEmpty()
						|| !hosts.stream().allMatch(OpenShellTransport::isValidHost)) {
					classLogger.warn("Project {} {} entry {} needs an upper-case name and plain host names", projectId,
							SECRETS_PROP, element);
					continue;
				}
				declared.add(new Declared(name, List.copyOf(new TreeSet<>(hosts))));
			} catch (RuntimeException e) {
				classLogger.warn("Project {} {} entry {} is not {\"name\", \"hosts\"}", projectId, SECRETS_PROP, element);
			}
		}
		return declared;
	}

	/** The project's key in credential names and the placeholder file: its first 8 id characters. */
	static String projectKey(String projectId) {
		String compact = projectId.toLowerCase().replaceAll("[^a-z0-9]", "");
		return compact.length() >= 8 ? compact.substring(0, 8) : String.format("%-8s", compact).replace(' ', '0');
	}

	/** The credential's name in the sandbox, unique per project so two apps may share a name. */
	static String credentialName(String projectId, String name) {
		return name + "__" + projectKey(projectId);
	}

	/**
	 * The profile and provider id. It includes a digest of the hosts and the interpreter, so a
	 * changed declaration is a new profile rather than an edit of one that sandboxes use.
	 */
	static String providerId(String projectId, Declared secret) {
		String hosts = digest(String.join(",", secret.hosts()) + "|" + OpenShellTransport.pythonBinary()).substring(0, 6);
		return "semoss-" + projectKey(projectId) + "-" + secret.name().toLowerCase().replace('_', '-') + "-" + hosts;
	}

	private static void ensureProfile(String id, String projectId, Declared secret, String credential)
			throws IOException, InterruptedException {
		try {
			OpenShellTransport.cli(List.of("profile", "describe", id), CLI_TIMEOUT_S, Map.of());
			return;
		} catch (IOException missing) {
			// not registered yet
		}
		StringBuilder yaml = new StringBuilder();
		yaml.append("id: ").append(id).append('\n');
		yaml.append("display_name: ").append(secret.name()).append(" (SEMOSS project ").append(projectId).append(")\n");
		yaml.append("description: sandbox secret declared by SEMOSS project ").append(projectId).append('\n');
		yaml.append("category: other\n");
		yaml.append("credentials:\n  - name: ").append(credential).append("\n    required: true\n");
		yaml.append("endpoints:\n");
		for (String host : secret.hosts()) {
			// credentialed endpoints must be inspected (L7) for the supervisor to swap the value in
			yaml.append("  - host: ").append(host).append("\n    port: 443\n    protocol: rest\n    access: full\n");
		}
		// without a binary the generated rule lets no program connect
		yaml.append("binaries:\n  - ").append(OpenShellTransport.pythonBinary()).append('\n');
		Path dir = Files.createTempDirectory("openshell-profile-");
		try {
			Path file = dir.resolve(id + ".yaml");
			Files.writeString(file, yaml.toString(), StandardCharsets.UTF_8);
			OpenShellTransport.cli(List.of("profile", "import", "--file", file.toString()), CLI_TIMEOUT_S, Map.of());
		} finally {
			deleteQuietly(dir);
		}
	}

	/**
	 * Create the provider, or update its value when it changed.
	 *
	 * @return whether the value was set, which starts a new placeholder generation
	 */
	private static boolean ensureProvider(String id, String credential, String value)
			throws IOException, InterruptedException {
		String digest = digest(value);
		Map<String, String> env = Map.of(credential, value);
		boolean exists;
		try {
			OpenShellTransport.cli(List.of("provider", "get", id), CLI_TIMEOUT_S, Map.of());
			exists = true;
		} catch (IOException missing) {
			exists = false;
		}
		if (exists && digest.equals(APPLIED.get(id))) {
			return false;
		}
		if (exists) {
			OpenShellTransport.cli(List.of("provider", "update", id, "--credential", credential), CLI_TIMEOUT_S, env);
		} else {
			OpenShellTransport.cli(List.of("provider", "create", "--name", id, "--type", id, "--credential", credential),
					CLI_TIMEOUT_S, env);
		}
		APPLIED.put(id, digest);
		return true;
	}

	/** The placeholder generation new processes in the sandbox get, or null with no secret yet. */
	private static String generation(String sandbox) throws IOException, InterruptedException {
		Matcher m = GENERATION.matcher(OpenShellTransport.cli(List.of("sandbox", "exec", "-n", sandbox, "--", "env"),
				CLI_TIMEOUT_S, Map.of()));
		return m.find() ? m.group(1) : null;
	}

	/** Wait for the generation an update or attach starts; the old one keeps the old value. */
	private static void awaitNewGeneration(String sandbox, String before) throws IOException, InterruptedException {
		long deadline = System.currentTimeMillis() + GENERATION_TIMEOUT_MS;
		while (System.currentTimeMillis() < deadline) {
			String now = generation(sandbox);
			if (now != null && !now.equals(before)) {
				return;
			}
			Thread.sleep(READY_POLL_MS);
		}
		classLogger.warn("Sandbox {} kept placeholder generation {} for {} s after a secret change", sandbox, before,
				GENERATION_TIMEOUT_MS / 1000);
	}

	private static String digest(String value) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	/** Wait until the sandbox reports the provider change applied; requests hang until then. */
	private static void awaitApplied(String sandbox, String id) throws IOException, InterruptedException {
		long deadline = System.currentTimeMillis() + READY_TIMEOUT_MS;
		String status = "";
		while (System.currentTimeMillis() < deadline) {
			status = OpenShellTransport.cli(List.of("sandbox", "provider", "status", sandbox, id), CLI_TIMEOUT_S, Map.of());
			if (status.contains(": ready")) {
				return;
			}
			Thread.sleep(READY_POLL_MS);
		}
		classLogger.warn("Sandbox {} did not report secret {} applied within {} s: {}", sandbox, id,
				READY_TIMEOUT_MS / 1000, status.lines().findFirst().orElse(""));
	}

	/**
	 * Write every brokered secret's current placeholder into the sandbox, by project key. The
	 * placeholders are not secret; they only resolve at the secret's own hosts.
	 */
	private static void writePlaceholders(String sandbox) throws IOException, InterruptedException {
		String env = OpenShellTransport.cli(List.of("sandbox", "exec", "-n", sandbox, "--", "env"), CLI_TIMEOUT_S,
				Map.of());
		Map<String, Map<String, String>> byProject = new TreeMap<>();
		for (String line : env.split("\\R")) {
			Matcher m = CREDENTIAL_ENV.matcher(line.trim());
			if (m.matches()) {
				byProject.computeIfAbsent(m.group(2), k -> new LinkedHashMap<>()).put(m.group(1), m.group(3));
			}
		}
		// A directory, not a lone file: for a file the CLI takes the destination as the file's
		// own name ("tar: insight: Cannot open: File exists"), while a directory lands inside it.
		Path dir = Files.createTempDirectory("openshell-secrets-");
		try {
			Path stage = Files.createDirectory(dir.resolve(PLACEHOLDER_DIR));
			Files.writeString(stage.resolve(PLACEHOLDER_FILE), new Gson().toJson(byProject), StandardCharsets.UTF_8);
			OpenShellTransport.cli(List.of("sandbox", "upload", sandbox, stage.toString(), PLACEHOLDER_PARENT),
					CLI_TIMEOUT_S, Map.of());
		} finally {
			deleteQuietly(dir);
		}
	}

	private static boolean containsWord(String text, String word) {
		return Pattern.compile("(^|\\s)" + Pattern.quote(word) + "(\\s|$)", Pattern.MULTILINE).matcher(text).find();
	}

	private static void deleteQuietly(Path dir) {
		try (var files = Files.walk(dir)) {
			files.sorted(Comparator.reverseOrder()).forEach(p -> {
				try {
					Files.deleteIfExists(p);
				} catch (IOException ignored) {
					// temp file
				}
			});
		} catch (IOException ignored) {
			// temp dir
		}
	}
}
