/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 * Licensed under the Apache License, Version 2.0.
 *******************************************************************************/
package prerna.reactor.agent.skill;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SkillScannerAgentIsolationUnitTests {

	@TempDir
	Path tempDir;

	@Test
	void managedSkillsAreVisibleOnlyToAgentsThatAttachThem() throws Exception {
		createSkill("pptx", "pptx-skill", true);
		createSkill("room-authored", null, false);

		var orchestratorSkills = SkillScanner.scan(tempDir.toString(), false, false, Set.of());
		var pptxSkills = SkillScanner.scan(tempDir.toString(), false, false, Set.of("pptx-skill"));

		assertEquals(List.of("room-authored"),
				orchestratorSkills.stream().map(SkillScanner.DiscoveredSkill::getName).sorted().toList());
		assertEquals(List.of("pptx", "room-authored").stream().sorted().toList(),
				pptxSkills.stream().map(SkillScanner.DiscoveredSkill::getName).sorted().toList());
	}

	private void createSkill(String slug, String skillId, boolean managed) throws Exception {
		Path directory = tempDir.resolve(".claude/skills").resolve(slug);
		Files.createDirectories(directory);
		Files.writeString(directory.resolve("SKILL.md"), "# " + slug, StandardCharsets.UTF_8);
		if (managed) {
			Files.writeString(directory.resolve(".skill-meta"),
					"{\"skill_id\":\"" + skillId + "\"}", StandardCharsets.UTF_8);
		}
	}
}
