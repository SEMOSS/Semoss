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

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.auth.User;
import prerna.project.impl.ProjectHelper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

/**
 * Creates a new skill.
 *
 * <p>
 * A skill is a Project of type {@code SKILL}. This reactor:
 * <ol>
 * <li>Creates the underlying Project via
 * {@link ProjectHelper#createSkillProject}.</li>
 * <li>Writes {@code SKILL.md} (and helper files, if any) into
 * {@code <project>/version/assets/skill/}.</li>
 * </ol>
 *
 * <p>
 * Inputs:
 * <ul>
 * <li>{@code skillContent} - SKILL.md body, with or without a YAML frontmatter
 * block (required)</li>
 * <li>{@code name} - display name. Required <i>only</i> when the frontmatter
 * omits one. When both are supplied, frontmatter wins.</li>
 * <li>{@code description} - same rule as {@code name}: required only when
 * frontmatter omits it.</li>
 * </ul>
 *
 * <p>
 * When the supplied {@code skillContent} already starts with a {@code ---}
 * frontmatter block it is written to disk verbatim. When it does not, a
 * frontmatter block is synthesized from {@code name} and {@code description}
 * and prepended before write, so the file on disk is always self-describing.
 *
 * <p>
 * Returns the newly assigned {@code skillId} (== the underlying project id).
 */
public class CreateSkillReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateSkillReactor.class);

	private static final String SKILL_CONTENT = "skillContent";

	public CreateSkillReactor() {
		this.keysToGet = new String[] { SKILL_CONTENT, ReactorKeysEnum.NAME.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String skillContent = this.keyValue.get(SKILL_CONTENT);
		String nameInput = this.keyValue.get(ReactorKeysEnum.NAME.getKey());
		String descInput = this.keyValue.get(ReactorKeysEnum.DESCRIPTION.getKey());

		if (skillContent == null || skillContent.isEmpty()) {
			throw new IllegalArgumentException("skillContent is required");
		}

		// Frontmatter wins when present; params are the fallback. The file on disk
		// is left alone if it already carries a frontmatter block (even if partial)
		// and gets a synthesized block prepended only when one is entirely absent.
		Skill.Frontmatter fm = Skill.parseFrontmatter(skillContent);
		boolean hasFrontmatter = Skill.hasFrontmatterBlock(skillContent);
		String name = firstNonEmpty(fm.name, nameInput);
		String description = firstNonEmpty(fm.description, descInput);
		if (name == null) {
			throw new IllegalArgumentException(
					"name is required: provide a 'name' input, or include 'name' in the SKILL.md frontmatter");
		}
		if (name.contains("/") || name.contains("\\") || name.contains("..")) {
			throw new IllegalArgumentException("Skill name must not contain path separators or '..'");
		}
		if (description == null) {
			throw new IllegalArgumentException("description is required: provide a 'description' input, "
					+ "or include 'description' in the SKILL.md frontmatter");
		}

		User user = this.insight.getUser();

		String skillId = GUID.v7().toString();
		String slug = Skill.slugify(name);
		String contentToWrite = hasFrontmatter ? skillContent
				: Skill.buildFrontmatter(name, description) + skillContent;

		try {
			ProjectHelper.createSkillProject(skillId, name, /* global */ false, /* gitProvider */ null,
					/* gitCloneUrl */ null, user, classLogger);

			String assetsFolder = AssetUtility.getProjectAssetsFolder(skillId);
			File skillDir = new File(assetsFolder, Skill.SKILL_ASSET_SUBFOLDER);
			if (!skillDir.exists() && !skillDir.mkdirs()) {
				throw new IllegalStateException("Failed to create skill content folder: " + skillDir.getAbsolutePath());
			}
			Path skillFile = skillDir.toPath().resolve(Skill.SKILL_FILE);
			Files.write(skillFile, contentToWrite.getBytes(StandardCharsets.UTF_8));
		} catch (Exception e) {
			classLogger.error("Failed to create skill '{}' (id {})", name, skillId, e);
			throw new IllegalArgumentException("Failed to create skill: " + e.getMessage(), e);
		}

		Map<String, Object> response = new HashMap<>();
		response.put("skill_id", skillId);
		response.put("project_id", skillId);
		response.put("slug", slug);
		response.put("name", name);
		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	private static String firstNonEmpty(String a, String b) {
		if (a != null && !a.isEmpty()) {
			return a;
		}
		if (b != null && !b.isEmpty()) {
			return b;
		}
		return null;
	}

	@Override
	public String getReactorDescription() {
		return "Creates a new skill (a SKILL-type Project) and writes its SKILL.md into the project's version/assets/skill/ folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (SKILL_CONTENT.equals(key)) {
			return "SKILL.md body, with or without a YAML frontmatter block. "
					+ "A frontmatter block will be synthesized from 'name' and 'description' if absent";
		}
		if (ReactorKeysEnum.NAME.getKey().equals(key)) {
			return "Display name. Required only when the SKILL.md frontmatter omits 'name'. "
					+ "Frontmatter wins when both are supplied";
		}
		if (ReactorKeysEnum.DESCRIPTION.getKey().equals(key)) {
			return "Description. Required only when the SKILL.md frontmatter omits 'description'. "
					+ "Frontmatter wins when both are supplied";
		}
		return super.getDescriptionForKey(key);
	}
}
