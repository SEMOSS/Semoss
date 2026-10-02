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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;
import prerna.io.connector.ms.MicrosoftGraphAppTokenProvider;
import prerna.io.connector.ms.MicrosoftGraphDriveClient;

/**
 * Storage engine backed by a Microsoft Teams channel's file store.
 *
 * <p>
 * Channel files are not a Teams resource; a channel maps to a folder in the
 * team's SharePoint document library. On open the team and channel are resolved
 * to that folder, and it becomes the root of this engine, so every storage path
 * is relative to the channel. This mirrors how the Azure engine behaves when
 * its SAS url names a single container. Leaving the channel blank roots the
 * engine at the team's document library instead, which makes every channel
 * visible as a top level folder.
 * </p>
 *
 * <p>
 * Required <b>application</b> Graph permissions, with admin consent granted:
 * </p>
 * <ul>
 * <li>{@code Team.ReadBasic.All} to resolve a team by name</li>
 * <li>{@code Channel.ReadBasic.All} to resolve a channel by name</li>
 * <li>{@code Files.ReadWrite.All} for the channel folder and all file
 * operations. {@code Files.Read.All} is enough for a read only engine.</li>
 * </ul>
 *
 * <p>
 * <b>{@code Sites.Selected} does not work with this engine.</b> That permission
 * only authorizes requests addressed under {@code /sites/{id}}, and this engine
 * resolves its root through {@code /groups} and {@code /teams}, which are not
 * gated by it and instead require the tenant wide permissions above. To scope a
 * service identity to individual teams, use {@link SharePointStorageEngine}
 * pointed at the team's site url, which stays on the site addressed route
 * throughout.
 * </p>
 *
 * @see AbstractMicrosoftGraphStorageEngine for every file operation
 * @see SharePointStorageEngine for a {@code Sites.Selected} compatible setup
 */
public class MicrosoftTeamsStorageEngine extends AbstractMicrosoftGraphStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftTeamsStorageEngine.class);

	public static final String MS_TEAMS_TENANT = "MS_TEAMS_TENANT";
	public static final String MS_TEAMS_CLIENT_ID = "MS_TEAMS_CLIENT_ID";
	public static final String MS_TEAMS_CLIENT_SECRET = "MS_TEAMS_CLIENT_SECRET";
	public static final String MS_TEAMS_TEAM = "MS_TEAMS_TEAM";
	public static final String MS_TEAMS_CHANNEL = "MS_TEAMS_CHANNEL";
	public static final String MS_TEAMS_SCOPE = "MS_TEAMS_SCOPE";

	/**
	 * Channel ids are of the form {@code 19:...@thread.tacv2}, which is what tells
	 * a configured id apart from a display name.
	 */
	private static final String CHANNEL_ID_PREFIX = "19:";

	private String teamId;
	private String channelId;

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.MICROSOFT_TEAMS;
	}

	@Override
	protected MicrosoftGraphAppTokenProvider createTokenProvider(Properties smssProp) {
		// the constructor validates the credentials, so a misconfigured engine fails
		// on open rather than on the first file operation
		return new MicrosoftGraphAppTokenProvider(smssProp.getProperty(MS_TEAMS_TENANT),
				smssProp.getProperty(MS_TEAMS_CLIENT_ID), smssProp.getProperty(MS_TEAMS_CLIENT_SECRET),
				smssProp.getProperty(MS_TEAMS_SCOPE));
	}

	@Override
	protected DriveRoot resolveDriveRoot(Properties smssProp) throws Exception {
		String team = smssProp.getProperty(MS_TEAMS_TEAM);
		String channel = smssProp.getProperty(MS_TEAMS_CHANNEL);
		if (isBlank(team)) {
			throw new IllegalArgumentException(
					MS_TEAMS_TEAM + " is required, as either the team display name or the team id.");
		}

		this.teamId = resolveTeamId(team);
		this.channelId = isBlank(channel) ? null : resolveChannelId(this.teamId, channel);

		Map<String, Object> root = this.channelId == null ? readTeamDriveRoot() : readChannelFilesFolder();
		return new DriveRoot(requireString(extractDriveId(root), "Unable to resolve the SharePoint drive for " + team),
				requireString(root.get(MicrosoftGraphDriveClient.ID), "Unable to resolve the root folder for " + team));
	}

	@Override
	protected String describeTarget(Properties smssProp) {
		String channel = smssProp.getProperty(MS_TEAMS_CHANNEL);
		return "team '" + smssProp.getProperty(MS_TEAMS_TEAM) + "' channel '"
				+ (isBlank(channel) ? "<team drive>" : channel) + "'";
	}

	/**
	 * Resolves a configured team to a team id, accepting either the id itself or a
	 * display name.
	 *
	 * <p>
	 * A team's id is its underlying group id, so a name lookup goes through
	 * {@code /groups} filtered to group backed teams. {@code /me/joinedTeams} is
	 * unavailable here because an app-only token has no signed in user.
	 * </p>
	 */
	private String resolveTeamId(String team) throws Exception {
		String value = team.trim();
		if (looksLikeGuid(value)) {
			return value;
		}

		List<Map<String, Object>> matches = findTeamsByName(value);
		if (matches.isEmpty()) {
			throw new IllegalArgumentException("No Microsoft Team found with the name '" + value
					+ "'. Check the name, or configure " + MS_TEAMS_TEAM + " with the team id instead.");
		}
		if (matches.size() > 1) {
			throw new IllegalArgumentException("More than one Microsoft Team is named '" + value + "'. Configure "
					+ MS_TEAMS_TEAM + " with the team id instead.");
		}
		return requireString(matches.get(0).get(MicrosoftGraphDriveClient.ID),
				"Microsoft Team '" + value + "' resolved without an id");
	}

	/**
	 * Finds the teams carrying a display name.
	 *
	 * <p>
	 * The server side filter is tried first, since it is one cheap request.
	 * Filtering on displayName is documented for {@code /teams}, but it is backed
	 * by the groups store and has historically been rejected on some tenants, so a
	 * rejection falls back to enumerating the teams and matching here. Both paths
	 * need only {@code Team.ReadBasic.All}.
	 * </p>
	 *
	 * <p>
	 * Filtering through {@code /groups} on {@code resourceProvisioningOptions} is
	 * deliberately avoided: that clause is an advanced query, so combining it with
	 * a displayName clause requires a {@code ConsistencyLevel: eventual} header
	 * plus {@code $count=true} and a broader directory permission.
	 * </p>
	 */
	private List<Map<String, Object>> findTeamsByName(String displayName) throws Exception {
		String filtered = GRAPH_BASE + "/teams?$filter="
				+ encodeQuery("displayName eq '" + escapeOData(displayName) + "'") + "&$select=id,displayName";
		try {
			List<Map<String, Object>> matches = graphList(filtered);
			if (!matches.isEmpty()) {
				return matches;
			}
		} catch (IllegalArgumentException e) {
			classLogger.warn("Filtering /teams by displayName was rejected, falling back to enumerating teams", e);
		}
		return matchTeamsByNameClientSide(displayName);
	}

	/**
	 * Enumerates the teams in the tenant and matches the display name here, for
	 * tenants where the server side filter is unavailable.
	 */
	private List<Map<String, Object>> matchTeamsByNameClientSide(String displayName) throws Exception {
		List<Map<String, Object>> matches = new ArrayList<>();
		// 999 is the largest page size Graph allows for directory backed collections.
		// It is a page size and not a cap, since graphListAllPages follows
		// @odata.nextLink to the end
		for (Map<String, Object> candidate : graphListAllPages(GRAPH_BASE + "/teams?$select=id,displayName&$top=999")) {
			Object name = candidate.get("displayName");
			if (name != null && name.toString().equalsIgnoreCase(displayName)) {
				matches.add(candidate);
			}
		}
		return matches;
	}

	/**
	 * Resolves a configured channel to a channel id, accepting either the id itself
	 * or a display name.
	 */
	private String resolveChannelId(String resolvedTeamId, String channel) throws Exception {
		String value = channel.trim();
		if (value.startsWith(CHANNEL_ID_PREFIX)) {
			return value;
		}

		// a team holds few channels, so they are listed and matched here rather than
		// through a server side filter, which keeps this off any filterable-property
		// restrictions
		List<Map<String, Object>> matches = new ArrayList<>();
		String url = GRAPH_BASE + "/teams/" + resolvedTeamId + "/channels?$select=id,displayName";
		for (Map<String, Object> candidate : graphListAllPages(url)) {
			Object name = candidate.get("displayName");
			if (name != null && name.toString().equalsIgnoreCase(value)) {
				matches.add(candidate);
			}
		}

		if (matches.isEmpty()) {
			throw new IllegalArgumentException("No channel named '" + value + "' exists in the configured team. "
					+ "Check the name, or configure " + MS_TEAMS_CHANNEL + " with the channel id instead.");
		}
		if (matches.size() > 1) {
			throw new IllegalArgumentException("More than one channel is named '" + value + "'. Configure "
					+ MS_TEAMS_CHANNEL + " with the channel id instead.");
		}
		return requireString(matches.get(0).get(MicrosoftGraphDriveClient.ID),
				"Channel '" + value + "' resolved without an id");
	}

	private Map<String, Object> readChannelFilesFolder() throws Exception {
		String url = GRAPH_BASE + "/teams/" + this.teamId + "/channels/" + this.channelId + "/filesFolder";
		Map<String, Object> folder = graphGet(url);
		if (folder == null) {
			throw new IllegalStateException("Graph returned no files folder for the configured channel.");
		}
		return folder;
	}

	private Map<String, Object> readTeamDriveRoot() throws Exception {
		String url = GRAPH_BASE + "/groups/" + this.teamId + "/drive/root?$select=id,name,parentReference,webUrl";
		Map<String, Object> root = graphGet(url);
		if (root == null) {
			throw new IllegalStateException("Graph returned no document library root for the configured team.");
		}
		return root;
	}
}
