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
package prerna.reactor.agent.skill;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Creates a new skill in the registry.
 *
 * <p>Inputs:
 * <ul>
 *   <li>{@code storage}              - id of the {@link IStorageEngine} that will hold the SKILL.md blob (required)</li>
 *   <li>{@code skillContent}         - full SKILL.md text including YAML frontmatter (required)</li>
 *   <li>{@code name}                 - display name; overrides frontmatter {@code name} when supplied (optional)</li>
 *   <li>{@code description}          - description; overrides frontmatter {@code description} when supplied (optional)</li>
 *   <li>{@code sharingEnabled}       - toggles SKILLPERMISSION checks, default false (optional)</li>
 *   <li>{@code status}               - initial status, default {@link Skill#STATUS_DRAFT} (optional)</li>
 *   <li>{@code origin}               - provenance, default {@link Skill#ORIGIN_USER}. Setting this to
 *                                      {@link Skill#ORIGIN_PLATFORM} requires platform admin (optional)</li>
 * </ul>
 *
 * <p>The directory-name slug is always derived server-side from the resolved {@code name}
 * via {@link Skill#slugify}; callers cannot override it so the on-disk layout stays
 * consistent across rooms and over time.
 *
 * <p>Side-effects:
 * <ol>
 *   <li>Uploads the SKILL.md to {@code skills/&lt;skillId&gt;/v1/SKILL.md} in the storage engine</li>
 *   <li>Inserts a {@code SKILL__} row and a {@code SKILL_VERSION__} row (version=1) in one transaction</li>
 * </ol>
 *
 * <p>Returns the newly assigned {@code skillId}.
 *
 * <p>SkillStager (step 5 of the build) will copy the staged blob back out to a
 * room's {@code .claude/skills/&lt;slug&gt;/} working dir at agent run time.
 */
public class CreateSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateSkillReactor.class);

	private static final String SKILL_CONTENT  = "skillContent";
	private static final String SHARING        = "sharingEnabled";
	private static final String STATUS         = "status";
	private static final String ORIGIN         = "origin";

	public CreateSkillReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.STORAGE.getKey(),
				SKILL_CONTENT,
				ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(),
				SHARING,
				STATUS,
				ORIGIN,
		};
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String storageEngineId = this.keyValue.get(ReactorKeysEnum.STORAGE.getKey());
		String skillContent    = this.keyValue.get(SKILL_CONTENT);
		String nameInput       = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String descInput       = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());
		boolean sharingEnabled  = parseBool(this.keyValue.get(SHARING), false);
		String status = orDefault(this.keyValue.get(STATUS), Skill.STATUS_DRAFT);
		String origin = orDefault(this.keyValue.get(ORIGIN), Skill.ORIGIN_USER);

		if (storageEngineId == null || storageEngineId.isEmpty()) {
			throw new IllegalArgumentException("storage is required");
		}
		if (skillContent == null || skillContent.isEmpty()) {
			throw new IllegalArgumentException("skillContent is required");
		}

		// Frontmatter is the source of truth for name + description; allow caller to
		// override either one explicitly (e.g. when name in DB should differ from the
		// frontmatter for display reasons).
		Skill.Frontmatter fm = Skill.parseFrontmatter(skillContent);
		String name = nameInput != null && !nameInput.isEmpty() ? nameInput : fm.name;
		String description = descInput != null && !descInput.isEmpty() ? descInput : fm.description;
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException(
					"SKILL.md must declare a 'name' in its frontmatter, or 'name' must be supplied as an input");
		}
		if (description == null || description.isEmpty()) {
			throw new IllegalArgumentException(
					"SKILL.md must declare a 'description' in its frontmatter, or 'description' must be supplied as an input");
		}

		User user = this.insight.getUser();
		String createdBy = resolveUserId(user);
		if (Skill.ORIGIN_PLATFORM.equals(origin)
				&& !Boolean.TRUE.equals(SecurityAdminUtils.userIsAdmin(user))) {
			throw new IllegalArgumentException("Only platform admins can create platform skills");
		}

		IStorageEngine storage = Utility.getStorage(storageEngineId);
		if (storage == null) {
			throw new IllegalArgumentException("Could not load storage engine with id: " + storageEngineId);
		}
		if (!SecurityEngineUtils.userCanEditEngine(user, storage.getEngineId())) {
			throw new IllegalArgumentException(
					"User does not have permission to write to the storage engine: " + storageEngineId);
		}

		String skillId = GUID.v7().toString();
		// Slug is always derived from the resolved name. Not exposed as a reactor
		// input - caller-supplied slugs would drift from frontmatter.name over time.
		String slug = Skill.slugify(name);
		String versionPrefix = "skills/" + skillId + "/v1";
		byte[] contentBytes = skillContent.getBytes(StandardCharsets.UTF_8);
		String hash = Skill.contentHash(contentBytes);
		long sizeBytes = contentBytes.length;

		Path tmpDir = null;
		Path tmpFile = null;
		try {
			// Stage SKILL.md in a temp dir so the storage engine preserves the basename.
			tmpDir = Files.createTempDirectory("skill-upload-");
			tmpFile = tmpDir.resolve(Skill.SKILL_FILE);
			Files.write(tmpFile, contentBytes);

			Map<String, Object> metadata = buildBlobMetadata(skillId, 1, hash, createdBy);
			storage.copyToStorage(tmpFile.toString(), versionPrefix, metadata);

			ModelInferenceLogsUtils.createNewSkill(skillId, slug, name, description, createdBy,
					sharingEnabled, storageEngineId, versionPrefix, versionPrefix, hash, sizeBytes, status, origin,
					/* configJson */ null);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to create skill: " + e.getMessage(), e);
		} finally {
			cleanup(tmpFile, tmpDir);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("skill_id", skillId);
		response.put("slug", slug);
		response.put("name", name);
		response.put("version", 1);
		response.put("content_hash", hash);
		response.put("storage_engine_id", storageEngineId);
		response.put("storage_prefix", versionPrefix);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static Map<String, Object> buildBlobMetadata(String skillId, int version, String hash, String createdBy) {
		Map<String, Object> md = new HashMap<>();
		md.put("skill_id", skillId);
		md.put("version", String.valueOf(version));
		md.put("content_hash", hash);
		if (createdBy != null) {
			md.put("created_by", createdBy);
		}
		return md;
	}

	private static void cleanup(Path tmpFile, Path tmpDir) {
		if (tmpFile != null) {
			try {
				Files.deleteIfExists(tmpFile);
			} catch (Exception e) {
				classLogger.warn("Failed to delete temp file {}: {}", tmpFile, e.getMessage());
			}
		}
		if (tmpDir != null) {
			try {
				Files.deleteIfExists(tmpDir);
			} catch (Exception e) {
				classLogger.warn("Failed to delete temp dir {}: {}", tmpDir, e.getMessage());
			}
		}
	}

	private static String resolveUserId(User user) {
		if (user == null || user.getLogins() == null || user.getLogins().isEmpty()) {
			return null;
		}
		AuthProvider login = user.getLogins().get(0);
		return user.getAccessToken(login) == null ? null : user.getAccessToken(login).getId();
	}

	private static boolean parseBool(String s, boolean defaultValue) {
		if (s == null || s.isEmpty()) {
			return defaultValue;
		}
		return Boolean.parseBoolean(s);
	}

	private static String orDefault(String s, String defaultValue) {
		return (s == null || s.isEmpty()) ? defaultValue : s;
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new skill: uploads its SKILL.md to a storage engine and records SKILL__ + SKILL_VERSION__ rows";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (ReactorKeysEnum.STORAGE.getKey().equals(key)) {
			return "Id of the storage engine that will hold the SKILL.md blob";
		}
		if (SKILL_CONTENT.equals(key)) {
			return "Full SKILL.md text including YAML frontmatter (name + description required)";
		}
		if (ReactorKeysEnum.NAME.getKey().equals(key)) {
			return "Display name; overrides frontmatter 'name' when supplied";
		}
		if (ReactorKeysEnum.DESCRIPTION.getKey().equals(key)) {
			return "Description; overrides frontmatter 'description' when supplied";
		}
		if (SHARING.equals(key)) {
			return "Toggle SKILLPERMISSION-based sharing. Default false";
		}
		if (STATUS.equals(key)) {
			return "Initial status: DRAFT | PUBLISHED | ARCHIVED | DEPRECATED. Default DRAFT";
		}
		if (ORIGIN.equals(key)) {
			return "Provenance: USER | PLATFORM | IMPORTED | GENERATED. Default USER. "
					+ "Setting to PLATFORM requires platform admin.";
		}
		return super.getDescriptionForKey(key);
	}
}
