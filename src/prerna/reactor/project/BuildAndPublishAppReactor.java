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
package prerna.reactor.project;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Pixel call: BuildAndPublishApp( project=['<projectId>'] );
 */
public class BuildAndPublishAppReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(BuildAndPublishAppReactor.class);

	private static final String PROJECT_ID = ReactorKeysEnum.PROJECT.getKey();
	private static final String CLIENT_DIR = "client";
	private static final String NODE_SERVER_ENDPOINT = "NODE_SERVER_ENDPOINT";
	private static final String BUILD_CMD = "pnpm install --no-frozen-lockfile && pnpm run build";
	private static final String INDEX_HTML = "index.html";

	/**
	 * Matches a {@code src="/..."} or {@code href="/..."} pointing at a script or
	 * stylesheet. Requiring a non-slash after the leading one excludes
	 * protocol-relative {@code //host/...} URLs. Only double-quoted attributes are
	 * considered, which is what every bundler emits.
	 */
	private static final Pattern ABSOLUTE_ASSET_URL = Pattern
			.compile("(?:src|href)=\"(/[^/\"][^\"]*\\.(?:js|mjs|css))\"", Pattern.CASE_INSENSITIVE);

	public BuildAndPublishAppReactor() {
		this.keysToGet = new String[] { PROJECT_ID };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = normalizeProjectId(user, this.keyValue.get(PROJECT_ID));

		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException(
					"Project does not exist, project id is invalid, or user is not an editor/author for this project");
		}

		IProject project = Utility.getProject(projectId);
		if (project == null) {
			throw new IllegalArgumentException("Could not find or load project = " + projectId);
		}

		String buildSvcUrl = Utility.getDIHelperProperty(NODE_SERVER_ENDPOINT);
		String endpointBase = normalizeBuildServiceUrl(buildSvcUrl);

		Path projectAssetsDir = Paths
				.get(AssetUtility.getProjectAssetsFolder(project.getProjectName(), project.getProjectId()))
				.toAbsolutePath().normalize();

		// Build always reads from assets/client
		Path clientDir = projectAssetsDir.resolve(CLIENT_DIR).normalize();

		if (!Files.isDirectory(clientDir)) {
			return getWarning("No client folder found at " + clientDir
					+ ". BuildAndPublishApp compiles client source from assets/client. For a project whose portal assets already run as-is, such as a plain index.html app, use PublishProject(project=['"
					+ projectId + "'], release=true).");
		}

		Path tempZip = null;
		Path portalsZip = null;

		try {
			// 1 - Zip the client folder
			tempZip = Files.createTempFile("semoss-client-", ".zip");
			zipDirectory(clientDir, tempZip);
			classLogger.info("Zipped client ({} bytes) -> {}", Files.size(tempZip), tempZip);

			// 2 - POST to the node builder service
			String endpoint = endpointBase + "/build" + "?buildCmd=" + URLEncoder.encode(BUILD_CMD, "UTF-8")
					+ "&outDir=" + URLEncoder.encode(Constants.PORTALS_FOLDER, "UTF-8");

			portalsZip = Files.createTempFile("semoss-portals-", ".zip");
			postMultipart(endpoint, tempZip, portalsZip);
			classLogger.info("Received portals zip ({} bytes)", Files.size(portalsZip));

			// 3 - Extract portals zip into the project's asset directory
			Path portalsDir = projectAssetsDir.resolve(Constants.PORTALS_FOLDER).normalize();
			deleteDirectory(portalsDir);
			Files.createDirectories(portalsDir);
			unzip(portalsZip, portalsDir, Constants.PORTALS_FOLDER);
			classLogger.info("Extracted portals -> {}", portalsDir);

			// 4 - Refuse to publish a build whose asset URLs are absolute. Serving would
			// 404 every asset and leave a blank page, so stop while the last good portal
			// is still the published one.
			String absoluteAsset = findAbsoluteAssetUrl(portalsDir);
			if (absoluteAsset != null) {
				classLogger.warn(
						"BuildAndPublishApp: absolute asset url '" + absoluteAsset + "' in " + Constants.PORTALS_FOLDER
								+ "/" + INDEX_HTML + " for project " + projectId + "; skipping publish");
				return getWarning("""
						Build completed but was not published: it produced absolute asset urls. \
						%s/%s references "%s", and a portal is served from a project-scoped path, \
						not from the server root, so that url 404s and the app renders a blank page. \
						Set base: "./" in client/vite.config.ts and build again. The portal currently \
						published for [%s] is unchanged.\
						""".formatted(Constants.PORTALS_FOLDER, INDEX_HTML, absoluteAsset, projectId));
			}

			// 5 - Re-publish and release the project so the public portal is refreshed.
			this.insight.runPixel("PublishProject(project='" + projectId + "', release=true);");

			// 6 - Push project to central storage so other pods see the changes
			try {
				ClusterUtil.pushProject(projectId);
			} catch (Exception e) {
				classLogger.error("BuildAndPublishApp: ClusterUtil.pushProject failed for project {}", projectId, e);
			}

			return new NounMetadata("App [" + projectId + "] built and published successfully.",
					PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);

		} catch (Exception e) {
			classLogger.error("BuildAndPublishApp failed for project {}", projectId, e);
			return getError(e.getMessage());
		} finally {
			quietDelete(tempZip);
			quietDelete(portalsZip);
		}
	}

	private void postMultipart(String url, Path zipFile, Path destFile) throws IOException, URISyntaxException {
		String boundary = "----SemossBuild" + Long.toHexString(System.currentTimeMillis());
		HttpURLConnection conn = (HttpURLConnection) new URI(url).toURL().openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		conn.setConnectTimeout(15_000);
		conn.setReadTimeout(600_000); // up to 10 min for slow builds
		conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

		try (OutputStream out = new BufferedOutputStream(conn.getOutputStream())) {
			String partHeader = "--" + boundary + "\r\n"
					+ "Content-Disposition: form-data; name=\"source\"; filename=\"client.zip\"\r\n"
					+ "Content-Type: application/zip\r\n\r\n";
			out.write(partHeader.getBytes("UTF-8"));
			Files.copy(zipFile, out);
			out.write(("\r\n--" + boundary + "--\r\n").getBytes("UTF-8"));
		}

		int status = conn.getResponseCode();
		if (status != 200) {
			String body;
			try (InputStream es = conn.getErrorStream()) {
				body = (es == null) ? "(no body)" : new String(es.readAllBytes(), "UTF-8");
			}
			throw new IOException("Build service returned HTTP " + status + ": " + body);
		}

		try (InputStream in = conn.getInputStream()) {
			Files.copy(in, destFile, StandardCopyOption.REPLACE_EXISTING);
		}
	}

	private void zipDirectory(Path dir, Path destZip) throws IOException {
		try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(destZip)))) {
			try (Stream<Path> stream = Files.walk(dir)) {
				stream.filter(p -> !Files.isDirectory(p)).forEach(p -> {
					String entryName = dir.relativize(p).toString().replace('\\', '/');
					try {
						zos.putNextEntry(new ZipEntry(entryName));
						Files.copy(p, zos);
						zos.closeEntry();
					} catch (IOException ex) {
						throw new UncheckedIOException(ex);
					}
				});
			}
		}
	}

	/**
	 * Unzip the portals archive; strips a leading "portals/" prefix if the service
	 * wraps entries under that folder name.
	 */
	private void unzip(Path zipFile, Path destDir, String stripPrefix) throws IOException {
		String prefix = stripPrefix.endsWith("/") ? stripPrefix : stripPrefix + "/";
		try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipFile)))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				if (entry.isDirectory()) {
					zis.closeEntry();
					continue;
				}
				String name = entry.getName().replace('\\', '/');
				if (name.startsWith(prefix)) {
					name = name.substring(prefix.length());
				}
				Path target = destDir.resolve(name).normalize();
				// Zip-slip guard
				if (!target.startsWith(destDir)) {
					throw new IOException("Zip slip detected: " + entry.getName());
				}
				Files.createDirectories(target.getParent());
				Files.copy(zis, target, StandardCopyOption.REPLACE_EXISTING);
				zis.closeEntry();
			}
		}
	}

	private void deleteDirectory(Path dir) throws IOException {
		if (!Files.exists(dir)) {
			return;
		}
		try (Stream<Path> stream = Files.walk(dir)) {
			stream.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
		}
	}

	private void quietDelete(Path p) {
		if (p != null) {
			try {
				Files.deleteIfExists(p);
			} catch (IOException ignored) {
			}
		}
	}

	private String normalizeProjectId(User user, String rawProjectId) {
		if (rawProjectId == null) {
			throw new IllegalArgumentException("Must input a project id");
		}
		String projectId = rawProjectId.trim();
		if (projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input a project id");
		}
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(user, projectId);
		return projectId;
	}

	private String normalizeBuildServiceUrl(String rawUrl) {
		if (rawUrl == null || rawUrl.trim().isEmpty()) {
			throw new IllegalArgumentException("Missing required property in RDF_Map.prop: " + NODE_SERVER_ENDPOINT);
		}
		URI uri = URI.create(rawUrl.trim());
		String scheme = uri.getScheme();
		if (scheme == null || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
			throw new IllegalArgumentException("buildServiceUrl must use http or https");
		}
		if (uri.getHost() == null || uri.getHost().trim().isEmpty()) {
			throw new IllegalArgumentException("buildServiceUrl must include a valid host");
		}
		return rawUrl.trim().replaceAll("/$", "");
	}

	/**
	 * Look for an absolute script or stylesheet url in the built index.html.
	 *
	 * A portal is served from a project-scoped path, so a reference such as
	 * {@code src="/assets/index-abc123.js"} resolves against the server root and
	 * always 404s, leaving a page that paints whatever index.html inlines and
	 * nothing else. Vite emits absolute urls unless {@code base} is set to
	 * {@code "./"}, and the build itself succeeds either way, which is what makes
	 * this worth catching here rather than at runtime.
	 *
	 * @param portalsDir the extracted build output
	 * @return the offending attribute value, or null when there is no index.html,
	 *         it cannot be read, or all of its asset urls are relative
	 */
	private String findAbsoluteAssetUrl(Path portalsDir) {
		Path indexHtml = portalsDir.resolve(INDEX_HTML).normalize();
		if (!Files.isRegularFile(indexHtml)) {
			return null;
		}
		try {
			Matcher matcher = ABSOLUTE_ASSET_URL.matcher(Files.readString(indexHtml, StandardCharsets.UTF_8));
			if (matcher.find()) {
				return matcher.group(1);
			}
		} catch (IOException e) {
			classLogger.warn("BuildAndPublishApp: could not read " + indexHtml + " to check asset urls", e);
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Build the app from assets/client using the configured builder service, write output to assets/portals, and publish the project. Output that could not be served, such as absolute asset urls from a missing base setting, is reported instead of published.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (PROJECT_ID.equals(key)) {
			return "The unique id (or alias) of the project to build and publish.";
		}
		return super.getDescriptionForKey(key);
	}
}
