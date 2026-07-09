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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Clones an existing skill into a new skill-project owned by the calling user.
 *
 * <p>
 * The clone is always {@code ORIGIN='USER'} regardless of the source's origin
 * (cloning a {@code PLATFORM} skill produces a personal copy, not another
 * platform skill - the latter would require admin and isn't the point of
 * cloning). The caller becomes the {@code CREATED_BY} and the owner of the
 * underlying project.
 *
 * <p>
 * What gets copied:
 * <ul>
 * <li>Every file under the source's {@code version/assets/skill/} folder
 * (SKILL.md + any helpers).</li>
 * <li>The description, carried into the clone's {@code SKILL__} row.</li>
 * </ul>
 *
 * <p>
 * What does <em>not</em> carry over:
 * <ul>
 * <li>Git history - the new project starts fresh.</li>
 * <li>Workspace attachments - the cloner can attach the clone wherever they
 * want via {@code AttachSkillToWorkspace}.</li>
 * <li>The source's owner / sharing.</li>
 * </ul>
 *
 * <p>
 * Inputs:
 * <ul>
 * <li>{@code skillId} - source skill identifier (required)</li>
 * <li>{@code name} - name for the new skill. Defaults to
 * {@code "Copy of <original>"}.</li>
 * </ul>
 *
 * <p>
 * Authorization: caller must be able to view the source skill-project
 * ({@link SecurityProjectUtils#userCanViewProject}).
 */
public class CloneSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CloneSkillReactor.class);

	private static final String SKILL_ID = "skillId";

	public CloneSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID, ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String sourceSkillId = this.keyValue.get(SKILL_ID);
		String nameInput = this.keyValue.get(ReactorKeysEnum.NAME.getKey());

		if (sourceSkillId == null || sourceSkillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanViewProject(user, sourceSkillId)) {
			throw new IllegalArgumentException(
					"Skill " + sourceSkillId + " does not exist or user does not have permission to view it");
		}

		Map<String, Object> sourceRow = ModelInferenceLogsUtils.getSkillEntry(sourceSkillId);
		if (sourceRow == null) {
			throw new IllegalArgumentException("Skill not found: " + sourceSkillId);
		}
		String sourceName = (String) sourceRow.get("name");
		String sourceDescription = (String) sourceRow.get("description");

		String newName = (nameInput != null && !nameInput.isEmpty()) ? nameInput : "Copy of " + sourceName;
		if (newName.contains("/") || newName.contains("\\") || newName.contains("..")) {
			throw new IllegalArgumentException("Skill name must not contain path separators or '..'");
		}
		String newSkillId = GUID.v7().toString();
		String newSlug = Skill.slugify(newName);
		String createdBy = resolveUserId(user);

		String sourceAssetsFolder = AssetUtility.getProjectAssetsFolder(sourceSkillId);
		Path sourceSkillDir = new File(sourceAssetsFolder, Skill.SKILL_ASSET_SUBFOLDER).toPath();
		if (!Files.isDirectory(sourceSkillDir)) {
			throw new IllegalStateException("Source skill content folder does not exist: " + sourceSkillDir);
		}

		try {
			// Clones are always personal copies - never carry origin=PLATFORM forward.
			ProjectHelper.createSkillProject(newSkillId, newName, /* global */ false, /* gitProvider */ null,
					/* gitCloneUrl */ null, user, classLogger);

			String newAssetsFolder = AssetUtility.getProjectAssetsFolder(newSkillId);
			File newSkillDir = new File(newAssetsFolder, Skill.SKILL_ASSET_SUBFOLDER);
			if (!newSkillDir.exists() && !newSkillDir.mkdirs()) {
				throw new IllegalStateException(
						"Failed to create skill content folder: " + newSkillDir.getAbsolutePath());
			}

			copyTree(sourceSkillDir, newSkillDir.toPath());

			// Re-synthesize the frontmatter so the file on disk matches the clone's
			// name (which may differ from the source) and stays in sync with the row.
			Path newSkillFile = newSkillDir.toPath().resolve(Skill.SKILL_FILE);
			String bodyOnly = Files.exists(newSkillFile)
					? Skill.stripFrontmatter(new String(Files.readAllBytes(newSkillFile), StandardCharsets.UTF_8))
					: "";
			String finalContent = Skill.buildFrontmatter(newName, sourceDescription) + bodyOnly;
			Files.write(newSkillFile, finalContent.getBytes(StandardCharsets.UTF_8));

			ModelInferenceLogsUtils.createNewSkill(newSkillId, newSlug, newName, sourceDescription, createdBy,
					Skill.ORIGIN_USER, /* configJson */ null);
		} catch (Exception e) {
			classLogger.error("Failed to clone skill into '{}' (id {})", newName, newSkillId, e);
			throw new IllegalArgumentException("Failed to clone skill: " + e.getMessage(), e);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("skill_id", newSkillId);
		response.put("project_id", newSkillId);
		response.put("name", newName);
		response.put("slug", newSlug);
		response.put("origin", Skill.ORIGIN_USER);
		response.put("source_skill_id", sourceSkillId);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static void copyTree(Path source, Path target) throws java.io.IOException {
		try (Stream<Path> walk = Files.walk(source)) {
			walk.forEach(src -> {
				try {
					Path dest = target.resolve(source.relativize(src).toString());
					if (Files.isDirectory(src)) {
						Files.createDirectories(dest);
					} else {
						Files.createDirectories(dest.getParent());
						Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
					}
				} catch (java.io.IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}

	private static String resolveUserId(User user) {
		if (user == null || user.getLogins() == null || user.getLogins().isEmpty()) {
			return null;
		}
		AuthProvider login = user.getLogins().get(0);
		return user.getAccessToken(login) == null ? null : user.getAccessToken(login).getId();
	}

	@Override
	public String getReactorDescription() {
		return "Clones a skill into a new skill-project owned by the caller (origin=USER)";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Identifier of the source skill to clone";
		}
		if (ReactorKeysEnum.NAME.getKey().equals(key)) {
			return "Name for the cloned skill. Defaults to 'Copy of <original>'";
		}
		return super.getDescriptionForKey(key);
	}
}
