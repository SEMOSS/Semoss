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
package prerna.project.impl;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.date.SemossDate;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.Settings;
import prerna.util.Utility;

/**
 * Owns a project's portal: whether the copy this container serves out of
 * public_home is still current, publishing it when it is not, and recording the
 * content changes that tell the other containers to do the same.
 *
 * <p>
 * A portal is served from public_home, which is local to the container handling
 * the request, while the content it is built from lives in the project's
 * assets/portals folder and is shared through central storage. Each container
 * therefore holds its own copy and has to work out on its own when that copy
 * has fallen behind.
 *
 * <p>
 * It does this by comparing two timestamps. The cluster timestamp is the
 * project's portal published timestamp in the security db, which every
 * container shares, and records when a container last reported a content
 * change. The local timestamp is {@link #getLastPublishDate()}, held in memory
 * here, and records when this container last copied that content into
 * public_home. A cluster timestamp newer than the local one means the copy this
 * container publishes is behind.
 *
 * <p>
 * Nothing inspects the portals folder itself, so a route that publishes portal
 * content has to report it through {@link #notePortalChange(User, String)}. A
 * change that goes unreported leaves the cluster timestamp untouched, and every
 * other container keeps serving the copy it already has.
 */
public class ProjectPortalsHelper {

	private static final Logger classLogger = LogManager.getLogger(ProjectPortalsHelper.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static final String PORTAL_INDEX_SCRIPT_ID = "semoss-env";
	private static final String PORTAL_SDK_IMPORTMAP_ID = "semoss-sdk-importmap";
	/** Path of the built SDK inside the FE webapp. */
	private static final String SDK_DIST_PATH = "/libs/sdk/dist";
	/** Entry file the "@semoss/sdk" bare specifier maps to. */
	private static final String SDK_ENTRY_FILE = "/index.mjs";

	private final IProject project;
	private final String projectPortalFolder;
	private final Properties smssProp;

	private SemossDate lastPortalPublishDate = null;
	private boolean publishedPortal = false;
	private boolean republishPortal = false;
	/**
	 * Whether this container has tried to give the project its first cluster
	 * timestamp. Tried, not succeeded: a container that cannot write one must not
	 * retry on every request for every asset a page serves.
	 */
	private boolean portalPublishTimestampInitAttempted = false;

	/**
	 * @param project             project whose portal this manages
	 * @param projectPortalFolder the project's assets/portals folder, null for a
	 *                            project that has no portal such as a user's asset
	 *                            project
	 * @param smssProp            the project's smss properties, read for the
	 *                            per-project COPY_PROJECT override
	 */
	public ProjectPortalsHelper(IProject project, String projectPortalFolder, Properties smssProp) {
		this.project = project;
		this.projectPortalFolder = projectPortalFolder;
		this.smssProp = smssProp;
	}

	/**
	 * Whether the portal has to be published into public_home before it is served,
	 * pulling the folder from central storage on the way when asked to.
	 *
	 * @param pullFromCloud whether to pull the portals folder when this container's
	 *                      copy may be behind
	 * @return true when the caller has to publish
	 */
	public boolean requirePublish(boolean pullFromCloud) {
		boolean outOfDate = portalIsOutOfDate();
		if (outOfDate || this.lastPortalPublishDate == null) {
			// just pull to make sure we have the latest in case project was loaded
			// but not published
			if (pullFromCloud) {
				classLogger.info(
						"Pulling Portals folder for project {}. Current portal out of date = {}. Last portal publish date = {}",
						this.project.getProjectId(), outOfDate, this.lastPortalPublishDate);
				ClusterUtil.pullProjectFolder(this.project, this.projectPortalFolder);
			}
		}

		// if this are true we want to republish
		// we just add the additional logic above if we have to pull from cloud
		return this.republishPortal || outOfDate || !this.publishedPortal;
	}

	/**
	 * Whether the copy of the portal this container publishes is behind the
	 * project's content.
	 *
	 * The cluster timestamp is the record of the latest change to a project's
	 * portals. A project with no cluster timestamp has never had a change reported,
	 * which leaves this comparison nothing to work from. Rather than keep asking a
	 * question that has no answer, the cluster timestamp is recorded once: that
	 * settles this container and reads as a change to any container whose local
	 * timestamp is older, resyncing those. Every later call takes the comparison
	 * above.
	 */
	private boolean portalIsOutOfDate() {
		String projectId = this.project.getProjectId();
		LocalDateTime clusterTimestamp = SecurityProjectUtils.getPortalPublishedTimestamp(projectId);
		if (clusterTimestamp != null) {
			return ProjectFreshness.isBehind(clusterTimestamp, this.lastPortalPublishDate);
		}

		if (!this.portalPublishTimestampInitAttempted) {
			this.portalPublishTimestampInitAttempted = true;
			if (SecurityProjectUtils.initPortalPublishedTimestamp(projectId)) {
				return true;
			}
			classLogger.error(
					"Could not record a cluster timestamp for project {}, so no container can tell whether the portal copy it publishes is current. Each will keep serving the copy it has until one is recorded.",
					projectId);
		}

		// nothing to compare against, and no way to record one, so republishing would
		// achieve nothing that serving the copy already published does not
		return false;
	}

	/**
	 * Publish the portals folder to public_home
	 *
	 * @param publicHomeFilePath this container's public_home directory
	 * @param pullFromCloud      whether to pull the portals folder first
	 * @return whether a published portal is in place afterwards
	 */
	public synchronized boolean publish(String publicHomeFilePath, boolean pullFromCloud) {
		if (publicHomeFilePath == null) {
			return false;
		}

		String projectId = this.project.getProjectId();
		String uniqueName = SmssUtilities.getUniqueName(this.project.getProjectName(), projectId);

		// find what is the final URL
		// this is the base url plus manipulations
		// find what the tomcat deploy directory is
		// no easy way to find other than may be find the classpath ? - will instrument
		// this through RDF Map
		boolean requirePublish = requirePublish(pullFromCloud);
		try {
			if (requirePublish) {
				Path sourcePortalsProjectPath = Paths.get(this.projectPortalFolder);
				Path targetPublicHomeProjectPortalsPath = Paths
						.get(publicHomeFilePath + DIR_SEPARATOR + projectId + DIR_SEPARATOR + Constants.PORTALS_FOLDER);

				File targetPublicHomeProjectPortalsDir = targetPublicHomeProjectPortalsPath.toFile();
				// if the target directory exists
				// we have to delete it before
				if (targetPublicHomeProjectPortalsDir.exists() && targetPublicHomeProjectPortalsDir.isDirectory()) {
					FileUtils.deleteDirectory(targetPublicHomeProjectPortalsDir);
				}

				rewritePortalIndexHtml(this.projectPortalFolder + DIR_SEPARATOR + "index.html");

				// do we physically copy of link?
				// first smss file
				// second rdf map
				boolean copy = true;
				if (this.smssProp != null && this.smssProp.getProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(this.smssProp.getProperty(Settings.COPY_PROJECT) + "");
				} else if (Utility.getDIHelperProperty(Settings.COPY_PROJECT) != null) {
					copy = Boolean.parseBoolean(Utility.getDIHelperProperty(Settings.COPY_PROJECT) + "");
				}

				// this is purely for testing purposes - this is because when eclipse publishes
				// it wipes the directory and removes the actual db
				if (copy) {
					if (!targetPublicHomeProjectPortalsDir.exists()) {
						targetPublicHomeProjectPortalsDir.mkdir();
					}
					FileUtils.copyDirectory(sourcePortalsProjectPath.toFile(), targetPublicHomeProjectPortalsDir);
				}
				// this is where we create symbolic link
				else if (!targetPublicHomeProjectPortalsDir.exists()
						&& !Files.isSymbolicLink(targetPublicHomeProjectPortalsPath)) {
					Files.createSymbolicLink(targetPublicHomeProjectPortalsPath, sourcePortalsProjectPath);
				}
				targetPublicHomeProjectPortalsDir.deleteOnExit();
				this.publishedPortal = true;
				this.republishPortal = false;
				this.lastPortalPublishDate = new SemossDate(Utility.getCurrentZonedDateTimeUTC());
				classLogger.info("Project '{}' has new last portal published date {}", uniqueName,
						this.lastPortalPublishDate);
			}
		} catch (Exception e) {
			classLogger.error("Failed to publish portals for project '{}'", uniqueName, e);
			this.publishedPortal = false;
			this.lastPortalPublishDate = null;
		}

		return this.publishedPortal;
	}

	/**
	 * Writes the platform's two auto-generated tags into the head of the portal's
	 * index.html:
	 *
	 * <pre>
	 * &lt;script id="semoss-sdk-importmap" type="importmap"&gt;
	 * {"imports":{"@semoss/sdk":"/{route - optional}/{fe webapp}/libs/sdk/dist/index.mjs"}}
	 * &lt;/script&gt;
	 * &lt;script id="semoss-env" type="application/json"&gt;
	 * {"APP": "&lt;project_id&gt;", "MODULE": "/{route - optional}/{context - usually just Monolith}"}
	 * &lt;/script&gt;
	 * </pre>
	 *
	 * The import map lets a portal built without a bundler write
	 * {@code import { Insight } from "@semoss/sdk"} in a module script, and the env
	 * script carries the app id and backend module the SDK reads on initialize.
	 * Both are keyed by element id so republishing updates them in place.
	 */
	private void rewritePortalIndexHtml(String indexHtmlPath) {
		File indexHtmlF = new File(indexHtmlPath);
		if (!indexHtmlF.exists() || !indexHtmlF.isFile()) {
			return;
		}

		String module = Utility.getApplicationRouteAndContextPath();
		org.jsoup.nodes.Document document;
		try {
			document = Jsoup.parse(indexHtmlF, "UTF-8");
			// pretty-printing re-indents the whole document which causes issues with
			// agent's editing an index.html because every change will affect future string
			// replacement attempts
			document.outputSettings().prettyPrint(false);
			Element head = document.selectFirst("head");
			if (head == null) {
				classLogger.warn("Portal index html has no head element, skipping rewrite {}",
						indexHtmlF.getAbsolutePath());
				return;
			}

			String scriptContent = "{\"APP\": \"" + this.project.getProjectId() + "\",\"MODULE\": \"" + module + "\"}";
			Element autoGenScript = document.getElementById(PORTAL_INDEX_SCRIPT_ID);
			if (autoGenScript == null) {
				head.prepend("<script id=\"" + PORTAL_INDEX_SCRIPT_ID + "\" type=\"application/json\">" + scriptContent
						+ "</script>");
			} else {
				autoGenScript.html(scriptContent);
			}

			writeSdkImportMap(document, head);

			String newHtml = document.html();
			try (FileWriter fw = new FileWriter(indexHtmlF, false)) {
				fw.write(newHtml);
				fw.flush();
			}
		} catch (Exception e) {
			classLogger.error("Failed to rewrite portal index html {}", indexHtmlF.getAbsolutePath(), e);
		}
	}

	/**
	 * Puts the SDK import map first in the head, where it precedes every module
	 * script on the page as the import map spec requires.
	 *
	 * <p>
	 * A document may only carry one import map, so a portal that ships its own
	 * (anything built through a bundler) is left alone and resolves
	 * {@code @semoss/sdk} through its own build instead.
	 */
	private static void writeSdkImportMap(org.jsoup.nodes.Document document, Element head) {
		String importMap = "{\"imports\":{\"@semoss/sdk\":\"" + resolveSdkEntryUrl() + "\"}}";
		Element existing = document.getElementById(PORTAL_SDK_IMPORTMAP_ID);
		if (existing != null) {
			existing.html(importMap);
		} else if (document.selectFirst("script[type=importmap]") == null) {
			head.prepend(
					"<script id=\"" + PORTAL_SDK_IMPORTMAP_ID + "\" type=\"importmap\">" + importMap + "</script>");
		}
	}

	/**
	 * URL the {@code @semoss/sdk} bare specifier resolves to in a published portal.
	 *
	 * <p>
	 * Points at the built SDK inside the FE webapp, which the war packages and
	 * serves as static content on the same origin as the portal, so the module and
	 * the page it is imported from always come from the same deployment.
	 */
	private static String resolveSdkEntryUrl() {
		String route = Utility.getApplicationOptionalRoutePath();
		String routePrefix = (route == null || route.isEmpty()) ? "" : "/" + route;
		return routePrefix + "/" + Utility.getFEWebAppName() + SDK_DIST_PATH + SDK_ENTRY_FILE;
	}

	/**
	 * Marks the portal for publishing the next time it is served, without deciding
	 * anything about the other containers.
	 */
	public void setRepublish(boolean republish) {
		this.republishPortal = republish;
	}

	/**
	 * @return whether this container has a published portal in place
	 */
	public boolean isPublished() {
		return this.publishedPortal;
	}

	/**
	 * @return the local timestamp, when this container last copied the portal
	 *         content into public_home, null when it never has
	 */
	public SemossDate getLastPublishDate() {
		return this.lastPortalPublishDate;
	}

	/**
	 * Records that a project's portal content changed, moving the cluster timestamp
	 * so every other container refreshes the portal copy it publishes.
	 *
	 * @param user      user performing the write
	 * @param projectId project that was written to
	 */
	public static void notePortalChange(User user, String projectId) {
		if (user == null || projectId == null || projectId.trim().isEmpty()) {
			return;
		}
		SecurityProjectUtils.setPortalPublish(user, projectId);
	}

}
