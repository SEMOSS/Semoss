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
package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.config.AgentConfig;

/** The agent's own files: read in place under agent:/, never written, copied into the room to be changed. */
class AgentFilesUnitTests {

	@TempDir
	Path room;

	@TempDir
	Path agent;

	@TempDir
	Path outside;

	private AgentRunContext ctx;
	private Map<String, PlatformAgentToolHandlers.ToolHandler> tools;

	@BeforeEach
	void agentWithFiles() throws Exception {
		Files.writeString(agent.resolve("guide.md"), "first line\nthe refund window is 30 days\n");
		Files.createDirectories(agent.resolve("templates"));
		Files.writeString(agent.resolve("templates/report.txt"), "Report for <name>\n");
		Files.writeString(outside.resolve("secret.txt"), "the refund window is a secret\n");
		ctx = contextWith(agent.toString());
		tools = PlatformAgentToolHandlers.handlersByName();
	}

	private AgentRunContext contextWith(String agentFilesDir) {
		AgentRunContext context = mock(AgentRunContext.class);
		when(context.getAgentConfig())
				.thenReturn(AgentConfig.builder().workingDir(room.toString()).agentFilesDir(agentFilesDir).build());
		return context;
	}

	@Test
	void readToolsReachAgentFilesAndNameThemWithThePrefix() throws Exception {
		assertTrue(tools.get("ReadFile").execute(Map.of("path", "agent:/guide.md"), ctx).contains("30 days"));
		assertTrue(tools.get("ReadFile").execute(Map.of("path", "agent:templates/report.txt"), ctx)
				.contains("Report for <name>"));

		String listed = tools.get("ListDirectory").execute(Map.of("path", "agent:/"), ctx);
		assertTrue(listed.contains("guide.md") && listed.contains("templates"), listed);

		String globbed = tools.get("GlobFiles").execute(Map.of("pattern", "*", "path", "agent:/"), ctx);
		assertTrue(globbed.lines().anyMatch("agent:/guide.md"::equals), globbed);
		assertTrue(globbed.lines().anyMatch("agent:/templates/report.txt"::equals), globbed);

		assertEquals("agent:/guide.md",
				tools.get("GrepFiles").execute(Map.of("pattern", "refund window", "path", "agent:/"), ctx));
		// the working directory is still what a search covers by default
		assertTrue(tools.get("GrepFiles").execute(Map.of("pattern", "refund window"), ctx).startsWith("No matches"));
	}

	@Test
	void aFileIsFoundUnderTheNameItsUploadStoredItBy() throws Exception {
		// an upload sanitizes the name, so "&" is on disk as "&amp;"; a model asks for the name as written
		Files.writeString(agent.resolve("R&amp;D plan.md"), "ship in Q3\n");
		Files.writeString(room.resolve("Q&amp;A.md"), "who asked\n");

		assertTrue(tools.get("ReadFile").execute(Map.of("path", "agent:/R&D plan.md"), ctx).contains("ship in Q3"));
		assertTrue(tools.get("ReadFile").execute(Map.of("path", "agent:/R&amp;D plan.md"), ctx).contains("ship in Q3"));
		assertTrue(tools.get("ReadFile").execute(Map.of("path", "Q&A.md"), ctx).contains("who asked"));
		assertEquals("Copied agent:/R&amp;D plan.md to R&amp;D plan.md",
				tools.get("CopyAgentFile").execute(Map.of("path", "agent:/R&D plan.md"), ctx));

		// the name as asked for wins when a file has it
		Files.writeString(agent.resolve("R&D plan.md"), "the other one\n");
		assertTrue(tools.get("ReadFile").execute(Map.of("path", "agent:/R&D plan.md"), ctx).contains("the other one"));
		assertEquals("Error: file not found: agent:/S&P.md",
				tools.get("ReadFile").execute(Map.of("path", "agent:/S&P.md"), ctx));
	}

	@Test
	void aDoubleStarPatternAlsoFindsTheFilesAtTheTop() throws Exception {
		String everything = tools.get("GlobFiles").execute(Map.of("pattern", "**/*", "path", "agent:/"), ctx);
		assertTrue(everything.lines().anyMatch("agent:/guide.md"::equals), everything);
		assertTrue(everything.lines().anyMatch("agent:/templates/report.txt"::equals), everything);

		assertEquals("agent:/guide.md",
				tools.get("GlobFiles").execute(Map.of("pattern", "**/*.md", "path", "agent:/"), ctx));
		assertEquals("agent:/guide.md", tools.get("GrepFiles")
				.execute(Map.of("pattern", "refund", "path", "agent:/", "glob", "**/*.md"), ctx));

		Files.writeString(room.resolve("notes.md"), "refund notes\n");
		assertEquals("notes.md", tools.get("GlobFiles").execute(Map.of("pattern", "**/*.md"), ctx));
	}

	@Test
	void agentFilesAreNeverWrittenAndNothingOutsideThemIsRead() throws Exception {
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("WriteFile").execute(Map.of("path", "agent:/guide.md", "content", "changed"), ctx));
		assertThrows(IllegalArgumentException.class, () -> tools.get("EditFile")
				.execute(Map.of("path", "agent:/guide.md", "old_string", "30", "new_string", "90"), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("DeleteFile").execute(Map.of("path", "agent:/guide.md"), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("MoveFile").execute(Map.of("path", "agent:/guide.md", "new_path", "guide.md"), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("MoveFile").execute(Map.of("path", "notes.md", "new_path", "agent:/notes.md"), ctx));
		assertEquals("first line\nthe refund window is 30 days\n", Files.readString(agent.resolve("guide.md")));
		assertFalse(Files.exists(room.resolve("agent:")));

		String leavesTheFolder = "agent:/../" + outside.getFileName() + "/secret.txt";
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("ReadFile").execute(Map.of("path", leavesTheFolder), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("CopyAgentFile").execute(Map.of("path", leavesTheFolder), ctx));

		// a link inside the agent's files that points outside them
		Files.createSymbolicLink(agent.resolve("link.txt"), outside.resolve("secret.txt"));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("ReadFile").execute(Map.of("path", "agent:/link.txt"), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("CopyAgentFile").execute(Map.of("path", "agent:/link.txt"), ctx));
		assertEquals("agent:/guide.md",
				tools.get("GrepFiles").execute(Map.of("pattern", "refund window", "path", "agent:/"), ctx));
		assertFalse(tools.get("GlobFiles").execute(Map.of("pattern", "*", "path", "agent:/"), ctx).contains("link"));
		assertFalse(SemossAgentHarness.buildAgentFilesPromptBlock(agent.toString()).contains("link.txt"));
	}

	@Test
	void copyPutsAnAgentFileInTheWorkingDirectoryAndKeepsWhatIsThere() throws Exception {
		assertEquals("Copied agent:/guide.md to guide.md",
				tools.get("CopyAgentFile").execute(Map.of("path", "agent:/guide.md"), ctx));
		tools.get("EditFile").execute(Map.of("path", "guide.md", "old_string", "30", "new_string", "90"), ctx);
		assertTrue(Files.readString(room.resolve("guide.md")).contains("90 days"));
		assertTrue(Files.readString(agent.resolve("guide.md")).contains("30 days"));

		assertEquals("Copied agent:/guide.md to guide (2).md (guide.md already exists)",
				tools.get("CopyAgentFile").execute(Map.of("path", "guide.md"), ctx));
		assertTrue(Files.readString(room.resolve("guide.md")).contains("90 days"));

		assertEquals("Copied agent:/guide.md to guide.md",
				tools.get("CopyAgentFile").execute(Map.of("path", "agent:/guide.md", "override", true), ctx));
		assertTrue(Files.readString(room.resolve("guide.md")).contains("30 days"));

		assertEquals("Copied agent:/templates/report.txt to drafts/q3.txt", tools.get("CopyAgentFile")
				.execute(Map.of("path", "agent:/templates/report.txt", "new_path", "drafts/q3.txt"), ctx));
		assertEquals("Report for <name>\n", Files.readString(room.resolve("drafts/q3.txt")));

		assertThrows(IllegalArgumentException.class,
				() -> tools.get("CopyAgentFile").execute(Map.of("path", "agent:/templates"), ctx));
		assertThrows(IllegalArgumentException.class,
				() -> tools.get("CopyAgentFile").execute(Map.of("path", "agent:/missing.md"), ctx));
		assertThrows(IllegalArgumentException.class, () -> tools.get("CopyAgentFile")
				.execute(Map.of("path", "agent:/guide.md", "new_path", "../guide.md"), ctx));
	}

	@Test
	void anAgentWithoutFilesKeepsTheWorkingDirectoryOnly() throws Exception {
		AgentRunContext plain = contextWith(null);
		assertEquals("Error: file not found: agent:/guide.md",
				tools.get("ReadFile").execute(Map.of("path", "agent:/guide.md"), plain));
		assertEquals("Error: this agent has no files of its own",
				tools.get("CopyAgentFile").execute(Map.of("path", "agent:/guide.md"), plain));
		assertEquals("", SemossAgentHarness.buildAgentFilesPromptBlock(null));
	}

	@Test
	void thePromptNamesTheAgentFilesButNoBookkeeping() throws Exception {
		Files.writeString(agent.resolve(".hidden"), "x");
		Files.createDirectories(agent.resolve(".cache"));
		Files.writeString(agent.resolve(".cache/index.json"), "{}");
		// what the file explorer leaves in a folder it creates
		Files.writeString(agent.resolve("placeholder.txt"), "placeholder");
		Files.writeString(agent.resolve("templates/placeholder.txt"), "placeholder");
		String block = SemossAgentHarness.buildAgentFilesPromptBlock(agent.toString());
		assertTrue(block.contains("- agent:/guide.md\n- agent:/templates/report.txt\n"), block);
		assertFalse(block.contains(".hidden") || block.contains(".cache") || block.contains("placeholder"), block);
		assertTrue(block.contains("CopyAgentFile"));

		Path empty = Files.createDirectories(outside.resolve("empty"));
		Files.writeString(empty.resolve("placeholder.txt"), "placeholder");
		assertEquals("", SemossAgentHarness.buildAgentFilesPromptBlock(empty.toString()));
	}
}
