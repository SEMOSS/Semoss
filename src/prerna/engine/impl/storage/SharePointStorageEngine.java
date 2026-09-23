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
package prerna.engine.impl.storage;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.MicrosoftGraphDriveClient;

/**
 * Storage engine backed by a SharePoint document library.
 *
 * <p>
 * Configure it with a site, optionally a document library, and optionally a
 * folder within that library. The resolved folder becomes the root of this
 * engine, so every storage path is relative to it and callers never deal with
 * site ids or drive ids.
 * </p>
 *
 * <p>
 * The site is given as the url an admin already has, for example
 * {@code https://contoso.sharepoint.com/sites/Marketing}, which is split into
 * the hostname and server relative path that Graph addresses sites by. A Graph
 * site id of the form {@code host,siteGuid,webGuid} is accepted as is, and a
 * bare hostname resolves to that tenant's root site.
 * </p>
 *
 * <p>
 * Leaving the library blank uses the site's default document library, which is
 * "Documents" (internally "Shared Documents") on a standard team site. The
 * library is matched on the display name an admin sees rather than on the
 * internal name used in urls.
 * </p>
 *
 * <p>
 * Upload conflicts are governed by the shared
 * {@link AbstractMicrosoftGraphStorageEngine#MS_CONFLICT_BEHAVIOR} property.
 * </p>
 *
 * <p>
 * Required <b>application</b> Graph permissions, with admin consent granted:
 * </p>
 * <ul>
 * <li>{@code Sites.Read.All} to resolve the site and enumerate its document
 * libraries. {@code Sites.ReadWrite.All} is needed to write.</li>
 * <li>{@code Files.Read.All} / {@code Files.ReadWrite.All} also satisfy the
 * drive item operations, if permissions are scoped by files rather than by
 * sites.</li>
 * </ul>
 *
 * <p>
 * For a tenant that restricts app-only access with Sites.Selected, the app has
 * to be granted access to this specific site; the site wide permissions above
 * are otherwise the simplest configuration.
 * </p>
 *
 * @see AbstractMicrosoftGraphStorageEngine for every file operation
 */
public class SharePointStorageEngine extends AbstractMicrosoftGraphStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(SharePointStorageEngine.class);

	public static final String SP_TENANT = "SP_TENANT";
	public static final String SP_CLIENT_ID = "SP_CLIENT_ID";
	public static final String SP_CLIENT_SECRET = "SP_CLIENT_SECRET";
	public static final String SP_SITE = "SP_SITE";
	public static final String SP_LIBRARY = "SP_LIBRARY";
	public static final String SP_FOLDER = "SP_FOLDER";
	public static final String SP_SCOPE = "SP_SCOPE";

	/**
	 * A Graph site id is {@code hostname,siteCollectionGuid,webGuid}, so a value
	 * carrying commas is already an id rather than a url.
	 */
	private static final String SITE_ID_DELIMITER = ",";

	private String siteId;

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.SHAREPOINT;
	}

	@Override
	protected MicrosoftGraphAppTokenProvider createTokenProvider(Properties smssProp) {
		// the constructor validates the credentials, so a misconfigured engine fails
		// on open rather than on the first file operation
		return new MicrosoftGraphAppTokenProvider(smssProp.getProperty(SP_TENANT), smssProp.getProperty(SP_CLIENT_ID),
				smssProp.getProperty(SP_CLIENT_SECRET), smssProp.getProperty(SP_SCOPE));
	}

	@Override
	protected DriveRoot resolveDriveRoot(Properties smssProp) throws Exception {
		String site = smssProp.getProperty(SP_SITE);
		String library = smssProp.getProperty(SP_LIBRARY);
		String folder = smssProp.getProperty(SP_FOLDER);
		if (isBlank(site)) {
			throw new IllegalArgumentException(SP_SITE + " is required, as either the site url, for example "
					+ "https://contoso.sharepoint.com/sites/Marketing, or a Graph site id.");
		}

		this.siteId = resolveSiteId(site);
		String resolvedDriveId = resolveDriveId(this.siteId, library);
		String resolvedRootItemId = resolveRootItemId(resolvedDriveId, folder);
		return new DriveRoot(resolvedDriveId, resolvedRootItemId);
	}

	@Override
	protected String describeTarget(Properties smssProp) {
		String library = smssProp.getProperty(SP_LIBRARY);
		String folder = smssProp.getProperty(SP_FOLDER);
		return "site '" + smssProp.getProperty(SP_SITE) + "' library '" + (isBlank(library) ? "<default>" : library)
				+ "'" + (isBlank(folder) ? "" : " folder '" + folder + "'");
	}

	/**
	 * Resolves the configured site to a Graph site id.
	 *
	 * <p>
	 * Accepts a Graph site id as is, a full site url, or a bare hostname. A url is
	 * split into hostname and server relative path, which Graph addresses as
	 * {@code /sites/{hostname}:/{path}}. A hostname on its own resolves to the
	 * tenant's root site.
	 * </p>
	 */
	private String resolveSiteId(String site) throws Exception {
		String value = site.trim();
		if (value.contains(SITE_ID_DELIMITER)) {
			// already a Graph site id
			return value;
		}

		String url = buildSiteUrl(value);
		Map<String, Object> resolved = graphGet(url + "?$select=id,displayName,webUrl");
		if (resolved == null) {
			throw new IllegalArgumentException("No SharePoint site found at " + site + ". Check " + SP_SITE + ".");
		}
		String resolvedId = requireString(resolved.get(MicrosoftGraphDriveClient.ID),
				"SharePoint site " + site + " resolved without an id");
		classLogger.debug("Resolved SharePoint site '{}' to {}", site, resolvedId);
		return resolvedId;
	}

	/**
	 * Builds the Graph url that addresses the configured site, splitting a url into
	 * the hostname and server relative path that Graph addresses sites by.
	 *
	 * @param site the configured site, as a url or a bare hostname
	 * @return the absolute Graph url for that site, with no query string
	 */
	private String buildSiteUrl(String site) throws Exception {
		String value = site.trim();
		String hostname;
		String serverRelativePath;
		if (value.startsWith("http://") || value.startsWith("https://")) {
			URI uri = new URI(value);
			hostname = uri.getHost();
			if (hostname == null) {
				throw new IllegalArgumentException("Unable to read a hostname out of " + SP_SITE + " = " + site);
			}
			serverRelativePath = uri.getPath() == null ? "" : uri.getPath();
		} else {
			// a bare hostname, or a hostname followed by a path
			int firstSlash = value.indexOf('/');
			hostname = firstSlash < 0 ? value : value.substring(0, firstSlash);
			serverRelativePath = firstSlash < 0 ? "" : value.substring(firstSlash);
		}

		while (serverRelativePath.startsWith("/")) {
			serverRelativePath = serverRelativePath.substring(1);
		}
		while (serverRelativePath.endsWith("/")) {
			serverRelativePath = serverRelativePath.substring(0, serverRelativePath.length() - 1);
		}

		// no path means the tenant's root site, which is addressed by hostname alone
		return serverRelativePath.isEmpty() ? GRAPH_BASE + "/sites/" + hostname
				: GRAPH_BASE + "/sites/" + hostname + ":/" + serverRelativePath;
	}

	/**
	 * Names the concrete request that looks up this site's id, so the guidance for
	 * a denied grant can be copied and run as it stands.
	 */
	@Override
	protected String describeSiteLookup(Properties smssProp) {
		String site = smssProp.getProperty(SP_SITE);
		if (isBlank(site)) {
			return super.describeSiteLookup(smssProp);
		}
		if (site.trim().contains(SITE_ID_DELIMITER)) {
			// already an id, so there is nothing to look up
			return "the site id is already configured as " + site.trim();
		}

		// the search route is used rather than the hostname:/path form, because the id
		// that the path form reports back is not always the one the permissions
		// endpoint wants, while search reports the id to use as it stands
		String siteName = lastPathSegment(site);
		return isBlank(siteName) ? super.describeSiteLookup(smssProp)
				: "GET " + GRAPH_BASE + "/sites?search=" + siteName;
	}

	/**
	 * The last path segment of the configured site, which is the site name to
	 * search on.
	 */
	private static String lastPathSegment(String site) {
		String value = site.trim();
		while (value.endsWith("/")) {
			value = value.substring(0, value.length() - 1);
		}
		int lastSlash = value.lastIndexOf('/');
		return lastSlash < 0 ? value : value.substring(lastSlash + 1);
	}

	/**
	 * Resolves the document library to a drive id, falling back to the site's
	 * default library when none is named.
	 */
	private String resolveDriveId(String resolvedSiteId, String library) throws Exception {
		if (isBlank(library)) {
			Map<String, Object> defaultDrive = graphGet(
					GRAPH_BASE + "/sites/" + resolvedSiteId + "/drive?$select=id,name");
			if (defaultDrive == null) {
				throw new IllegalStateException("Graph returned no default document library for the configured site.");
			}
			return requireString(defaultDrive.get(MicrosoftGraphDriveClient.ID),
					"The site's default document library resolved without an id");
		}

		String value = library.trim();
		List<Map<String, Object>> drives = graphList(
				GRAPH_BASE + "/sites/" + resolvedSiteId + "/drives?$select=id,name");
		// the library is matched on its display name, which is what an admin sees in
		// SharePoint, rather than on the internal name used in urls
		for (Map<String, Object> drive : drives) {
			Object name = drive.get(MicrosoftGraphDriveClient.NAME);
			if (name != null && name.toString().equalsIgnoreCase(value)) {
				return requireString(drive.get(MicrosoftGraphDriveClient.ID),
						"Document library '" + value + "' resolved without an id");
			}
		}

		StringBuilder available = new StringBuilder();
		for (Map<String, Object> drive : drives) {
			Object name = drive.get(MicrosoftGraphDriveClient.NAME);
			if (name == null) {
				continue;
			}
			if (available.length() > 0) {
				available.append(", ");
			}
			available.append(name);
		}
		throw new IllegalArgumentException("No document library named '" + value + "' exists on the configured site. "
				+ (available.length() == 0 ? "The site reports no document libraries."
						: "Available libraries: " + available));
	}

	/**
	 * Resolves the item that acts as this engine's root: either the drive's own
	 * root, or a folder beneath it when one is configured.
	 */
	private String resolveRootItemId(String resolvedDriveId, String folder) throws Exception {
		String url;
		if (isBlank(folder)) {
			url = GRAPH_BASE + "/drives/" + resolvedDriveId + "/root?$select=id,name";
		} else {
			String relativePath = folder.trim().replace('\\', '/');
			while (relativePath.startsWith("/")) {
				relativePath = relativePath.substring(1);
			}
			while (relativePath.endsWith("/")) {
				relativePath = relativePath.substring(0, relativePath.length() - 1);
			}
			url = GRAPH_BASE + "/drives/" + resolvedDriveId + "/root:/" + encodeFolderPath(relativePath)
					+ ":?$select=id,name,folder";
		}

		Map<String, Object> item = graphGet(url);
		if (item == null) {
			throw new IllegalArgumentException(
					"No folder found at " + SP_FOLDER + " = " + folder + " in the configured document library.");
		}
		if (!isBlank(folder) && !MicrosoftGraphDriveClient.isFolder(item)) {
			throw new IllegalArgumentException(
					SP_FOLDER + " = " + folder + " names a file rather than a folder. It has to be a folder, since it "
							+ "becomes the root of this engine.");
		}
		return requireString(item.get(MicrosoftGraphDriveClient.ID),
				"The configured root folder resolved without an id");
	}

	/**
	 * Encodes each segment of the configured folder path, keeping the separators
	 * intact.
	 */
	private static String encodeFolderPath(String path) {
		StringBuilder encoded = new StringBuilder();
		for (String segment : path.split("/")) {
			if (segment.isEmpty()) {
				continue;
			}
			if (encoded.length() > 0) {
				encoded.append("/");
			}
			encoded.append(encodeQuery(segment));
		}
		return encoded.toString();
	}
}
