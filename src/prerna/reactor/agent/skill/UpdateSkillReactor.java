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
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Updates an existing skill's {@code SKILL.md} content and/or description.
 *
 * <p>
 * The skill's <strong>name is immutable</strong> after creation - it backs both
 * the project alias and the on-disk slug used by every working dir that has
 * staged the skill, so renaming would invalidate caches and break stagers. To
 * rename, delete the skill and re-create it.
 *
 * <p>
 * The SKILL.md frontmatter is the source of truth for name/description: every
 * write synthesizes a fresh frontmatter block from the canonical name and the
 * resolved description, then appends the body. The caller's frontmatter (if
 * any) is read for its description but otherwise discarded.
 *
 * <p>
 * Inputs:
 * <ul>
 * <li>{@code skillId} - skill identifier, == the underlying project id
 * (required)</li>
 * <li>{@code skillContent} - new SKILL.md text. Frontmatter is optional - any
 * supplied frontmatter is replaced on write with the canonical block.
 * (optional)</li>
 * <li>{@code description} - new description. Wins over any frontmatter
 * description in {@code skillContent}. (optional)</li>
 * </ul>
 *
 * <p>
 * At least one of {@code skillContent} / {@code description} must be supplied.
 *
 * <p>
 * Authorization: caller must pass
 * {@link SecurityProjectUtils#userCanEditProject} on the skill-project.
 *
 * <p>
 * Does <em>not</em> commit the change to git; the existing project-commit
 * tooling owns that.
 */
public class UpdateSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateSkillReactor.class);

	private static final String SKILL_ID = "skillId";
	private static final String SKILL_CONTENT = "skillContent";

	public UpdateSkillReactor() {
		this.keysToGet = new String[] { SKILL_ID, SKILL_CONTENT, ReactorKeysEnum.DESCRIPTION.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String skillId = this.keyValue.get(SKILL_ID);
		String skillContent = this.keyValue.get(SKILL_CONTENT);
		String descInput = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());

		if (skillId == null || skillId.isEmpty()) {
			throw new IllegalArgumentException("skillId is required");
		}
		boolean hasContent = skillContent != null && !skillContent.isEmpty();
		boolean hasDescInput = descInput != null && !descInput.isEmpty();
		if (!hasContent && !hasDescInput) {
			throw new IllegalArgumentException("Nothing to update: pass skillContent and/or description");
		}

		User user = this.insight.getUser();
		if (!SecurityProjectUtils.userCanEditProject(user, skillId)) {
			throw new IllegalArgumentException(
					"Skill " + skillId + " does not exist or user does not have permission to edit it");
		}

		if (!SkillProjects.isSkillProject(skillId)) {
			throw new IllegalArgumentException("Skill not found: " + skillId);
		}
		SkillProjects.SkillInfo info = SkillProjects.resolve(skillId);
		String canonicalName = info.name;
		String currentDescription = info.description;

		Skill.Frontmatter incomingFm = hasContent ? Skill.parseFrontmatter(skillContent) : new Skill.Frontmatter();

		// Name is immutable - if the caller supplied a name in frontmatter that
		// disagrees with the canonical name, that's almost certainly a mistake.
		// Only enforceable when the current SKILL.md was actually readable; when
		// it is missing/unparseable the incoming content IS the repair path.
		if (info.found && incomingFm.name != null && !incomingFm.name.isEmpty()
				&& !incomingFm.name.equals(canonicalName)) {
			throw new IllegalArgumentException(
					"Skill name is immutable. Frontmatter declares '" + incomingFm.name + "' but the skill's name is '"
							+ canonicalName + "'. " + "Delete and recreate the skill to rename it.");
		}

		// Description resolution: explicit param wins, then incoming frontmatter,
		// then keep the existing value.
		String newDescription = hasDescInput ? descInput
				: (incomingFm.description != null && !incomingFm.description.isEmpty()) ? incomingFm.description
						: currentDescription;
		if (newDescription == null || newDescription.isEmpty()) {
			throw new IllegalArgumentException("Skill has no description and one was not provided; "
					+ "supply 'description' or include it in the frontmatter");
		}

		// Write back into the folder the SKILL.md actually lives in (skill/ or the
		// legacy public/ used by the shipped platform skill projects); when the file
		// is missing entirely, (re)create the standard skill/ folder.
		File skillDir = info.skillDir != null ? info.skillDir.toFile()
				: new File(AssetUtility.getProjectAssetsFolder(skillId), Skill.SKILL_ASSET_SUBFOLDER);
		Path skillFile = skillDir.toPath().resolve(Skill.SKILL_FILE);

		try {
			String body;
			if (hasContent) {
				body = Skill.stripFrontmatter(skillContent);
			} else {
				if (!Files.exists(skillFile)) {
					throw new IllegalStateException("Skill content file missing: " + skillFile);
				}
				String existing = new String(Files.readAllBytes(skillFile), StandardCharsets.UTF_8);
				body = Skill.stripFrontmatter(existing);
			}

			String finalContent = Skill.buildFrontmatter(canonicalName, newDescription) + body;
			if (!skillDir.exists() && !skillDir.mkdirs()) {
				throw new IllegalStateException("Failed to create skill content folder: " + skillDir.getAbsolutePath());
			}
			Files.write(skillFile, finalContent.getBytes(StandardCharsets.UTF_8));
		} catch (Exception e) {
			classLogger.error("Failed to update skill '{}'", skillId, e);
			throw new IllegalArgumentException("Failed to update skill: " + e.getMessage(), e);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("skill_id", skillId);
		response.put("name", canonicalName);
		response.put("description", newDescription);
		response.put("content_updated", hasContent);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		return "Updates a skill's SKILL.md body and/or description. Name is immutable.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_ID.equals(key)) {
			return "Identifier of the skill to update (== underlying project id)";
		}
		if (SKILL_CONTENT.equals(key)) {
			return "New SKILL.md body. Any supplied frontmatter is replaced on write with the "
					+ "canonical block built from the skill's immutable name and the resolved description";
		}
		if (ReactorKeysEnum.DESCRIPTION.getKey().equals(key)) {
			return "New description. Wins over any frontmatter description in skillContent";
		}
		return super.getDescriptionForKey(key);
	}
}
