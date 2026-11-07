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
package prerna.util.k8s;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.dataformat.yaml.YAMLMapper;

/**
 * The parts of a kubeconfig file needed to call one cluster: the API server,
 * the CA that signs its certificate, and the user's credentials (a client
 * certificate and key, or a bearer token). k3s writes a client certificate with
 * an EC key; tokens are what a ServiceAccount-based kubeconfig carries.
 *
 * Credential plugins ({@code exec}, {@code auth-provider}) and basic auth are
 * rejected with a clear message rather than half-supported.
 */
public final class KubeConfig {

	private static final YAMLMapper YAML = new YAMLMapper();

	private final Path source;
	private final String contextName;
	private final String server;
	private final String certificateAuthorityPem;
	private final String clientCertificatePem;
	private final String clientKeyPem;
	private final String token;
	private final Path tokenFile;
	private final String namespace;

	private KubeConfig(Path source, String contextName, String server, String certificateAuthorityPem,
			String clientCertificatePem, String clientKeyPem, String token, Path tokenFile, String namespace) {
		this.source = source;
		this.contextName = contextName;
		this.server = server;
		this.certificateAuthorityPem = certificateAuthorityPem;
		this.clientCertificatePem = clientCertificatePem;
		this.clientKeyPem = clientKeyPem;
		this.token = token;
		this.tokenFile = tokenFile;
		this.namespace = namespace;
	}

	/**
	 * @param file        the kubeconfig
	 * @param contextName the context to use, or null for its current-context
	 */
	public static KubeConfig load(Path file, String contextName) {
		if (file == null || !Files.isRegularFile(file)) {
			throw new IllegalArgumentException("Kubeconfig not found: " + file);
		}
		Map<String, Object> root;
		try {
			root = YAML.readValue(file.toFile(), new TypeReference<Map<String, Object>>() {
			});
		} catch (JacksonException e) {
			throw new IllegalArgumentException("Kubeconfig " + file + " is not valid YAML: " + e.getOriginalMessage(),
					e);
		}
		return parse(root, file, contextName);
	}

	static KubeConfig parse(Map<String, Object> root, Path source, String requestedContext) {
		if (root == null) {
			throw new IllegalArgumentException("Kubeconfig " + source + " is empty");
		}
		String contextName = requestedContext != null && !requestedContext.isBlank() ? requestedContext.strip()
				: string(root, "current-context");
		if (contextName == null) {
			throw new IllegalArgumentException(
					"Kubeconfig " + source + " has no current-context; name one with KSERVE_KUBE_CONTEXT");
		}
		Map<String, Object> context = named(root, "contexts", contextName, "context", source);
		Map<String, Object> cluster = named(root, "clusters", string(context, "cluster"), "cluster", source);
		String userName = string(context, "user");
		Map<String, Object> user = userName == null ? Map.of() : named(root, "users", userName, "user", source);

		String server = string(cluster, "server");
		if (server == null) {
			throw new IllegalArgumentException("Kubeconfig " + source + " has no server for context " + contextName);
		}
		if (Boolean.TRUE.equals(cluster.get("insecure-skip-tls-verify"))) {
			throw new IllegalArgumentException("Kubeconfig " + source
					+ " sets insecure-skip-tls-verify, which is not supported; give the cluster's certificate-authority-data instead");
		}
		for (String unsupported : List.of("exec", "auth-provider", "username", "password")) {
			if (user.containsKey(unsupported)) {
				throw new IllegalArgumentException("Kubeconfig " + source + " authenticates user " + userName + " with "
						+ unsupported + ", which is not supported; use a client certificate or a token");
			}
		}

		String ca = dataOrFile(cluster, "certificate-authority-data", "certificate-authority", source);
		String clientCert = dataOrFile(user, "client-certificate-data", "client-certificate", source);
		String clientKey = dataOrFile(user, "client-key-data", "client-key", source);
		String token = string(user, "token");
		String tokenFileValue = string(user, "tokenFile");
		Path tokenFile = tokenFileValue == null ? null : resolve(source, tokenFileValue);

		if ((clientCert == null) != (clientKey == null)) {
			throw new IllegalArgumentException("Kubeconfig " + source + " gives user " + userName
					+ " a client certificate without its key, or a key without its certificate");
		}
		if (clientCert == null && token == null && tokenFile == null) {
			throw new IllegalArgumentException(
					"Kubeconfig " + source + " has no client certificate or token for user " + userName);
		}
		return new KubeConfig(source, contextName, server, ca, clientCert, clientKey, token, tokenFile,
				string(context, "namespace"));
	}

	private static Map<String, Object> named(Map<String, Object> root, String listKey, String name, String entryKey,
			Path source) {
		if (name != null && root.get(listKey) instanceof List<?> entries) {
			for (Object entry : entries) {
				if (entry instanceof Map<?, ?> map && name.equals(map.get("name"))
						&& map.get(entryKey) instanceof Map<?, ?> value) {
					@SuppressWarnings("unchecked")
					Map<String, Object> typed = (Map<String, Object>) value;
					return typed;
				}
			}
		}
		throw new IllegalArgumentException("Kubeconfig " + source + " has no " + entryKey + " named " + name);
	}

	private static String string(Map<String, Object> map, String key) {
		Object value = map.get(key);
		if (value == null) {
			return null;
		}
		String text = value.toString().strip();
		return text.isEmpty() ? null : text;
	}

	private static String dataOrFile(Map<String, Object> map, String dataKey, String fileKey, Path source) {
		String data = string(map, dataKey);
		if (data != null) {
			try {
				return new String(Base64.getMimeDecoder().decode(data), StandardCharsets.UTF_8);
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException("Kubeconfig " + source + " has invalid base64 in " + dataKey, e);
			}
		}
		String file = string(map, fileKey);
		if (file == null) {
			return null;
		}
		Path path = resolve(source, file);
		try {
			return Files.readString(path, StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new IllegalArgumentException("Cannot read " + fileKey + " " + path + " named in kubeconfig " + source,
					e);
		}
	}

	/** Relative paths in a kubeconfig are relative to the kubeconfig's folder, as in kubectl. */
	private static Path resolve(Path source, String file) {
		Path path = Path.of(file);
		if (path.isAbsolute() || source == null || source.getParent() == null) {
			return path;
		}
		return source.getParent().resolve(path);
	}

	public Path getSource() {
		return this.source;
	}

	public String getContextName() {
		return this.contextName;
	}

	public String getServer() {
		return this.server;
	}

	/** @return the PEM of the CA bundle, or null to trust the JVM's default CAs */
	public String getCertificateAuthorityPem() {
		return this.certificateAuthorityPem;
	}

	public String getClientCertificatePem() {
		return this.clientCertificatePem;
	}

	public String getClientKeyPem() {
		return this.clientKeyPem;
	}

	public String getToken() {
		return this.token;
	}

	public Path getTokenFile() {
		return this.tokenFile;
	}

	/** @return the context's default namespace, or null */
	public String getNamespace() {
		return this.namespace;
	}

	public String describe() {
		return "kubeconfig " + this.source + " (context " + this.contextName + ")";
	}
}
