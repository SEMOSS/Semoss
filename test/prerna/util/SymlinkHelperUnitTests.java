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
package prerna.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.util.sandbox.SandboxTransporter;

/**
 * How a project reaches a user's chroot. A user who can edit the project works
 * on the project's own folder through a symlink. Any other user gets a copy
 * that nobody can write to. The main JVM compiles and loads custom reactors
 * and serves portals from the project's own folder, so the copy leaves those
 * folders out.
 */
class SymlinkHelperUnitTests {

	private static final String PROJECT_ID = "1234-abcd";

	@TempDir
	Path folder;

	@ParameterizedTest
	@ValueSource(strings = { "assets/java", "assets/classes", "assets/portals", ".git" })
	void aReadOnlyCopyLeavesOutTheReactorSourceTheClassesThePortalsAndTheGitFolder(String leftOut)
			throws IOException {
		Path appRoot = this.folder.resolve("app_root");
		write(appRoot.resolve("version/assets/kept.txt"));
		write(appRoot.resolve("version/" + leftOut + "/top.txt"));
		write(appRoot.resolve("version/" + leftOut + "/nested/deep/inner.txt"));
		Path target = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyProjectForReadOnlyUser(appRoot, target);

		assertTrue(Files.isRegularFile(target.resolve("version/assets/kept.txt")));
		assertFalse(Files.exists(target.resolve("version/" + leftOut)));
	}

	@Test
	void aReadOnlyCopyKeepsEveryOtherFileOfTheProject() throws IOException {
		Path appRoot = this.folder.resolve("app_root");
		write(appRoot.resolve("version/assets/py/script.py"));
		write(appRoot.resolve("version/assets/mcp/tools.json"));
		write(appRoot.resolve("version/assets/public/logo.txt"));
		write(appRoot.resolve("version/assets/data.csv"));
		write(appRoot.resolve("version/insight-id/recipe.json"));
		write(appRoot.resolve("version/.gitignore"));
		Files.createDirectories(appRoot.resolve("version/assets/empty"));
		Path target = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyProjectForReadOnlyUser(appRoot, target);

		for (String kept : new String[] { "version/assets/py/script.py", "version/assets/mcp/tools.json",
				"version/assets/public/logo.txt", "version/assets/data.csv", "version/insight-id/recipe.json",
				"version/.gitignore" }) {
			assertArrayEquals(Files.readAllBytes(appRoot.resolve(kept)), Files.readAllBytes(target.resolve(kept)),
					kept);
		}
		assertTrue(Files.isDirectory(target.resolve("version/assets/empty")));
	}

	@ParameterizedTest
	@ValueSource(strings = { "version/assets/py/java/helper.py", "version/assets/py/classes/model.py",
			"version/assets/data/portals/index.html", "version/java/notes.txt", "version/insight-id/classes/a.txt",
			"portals/readme.txt", "version/assets/py/.git/config", ".git/config" })
	void aFolderOfTheSameNameSomewhereElseIsStillCopied(String sameNameElsewhere) throws IOException {
		Path appRoot = this.folder.resolve("app_root");
		write(appRoot.resolve(sameNameElsewhere));
		Path target = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyProjectForReadOnlyUser(appRoot, target);

		assertTrue(Files.isRegularFile(target.resolve(sameNameElsewhere)));
	}

	@Test
	void aFileNamedLikeOneOfTheFoldersIsStillCopied() throws IOException {
		Path appRoot = this.folder.resolve("app_root");
		write(appRoot.resolve("version/assets/java.md"));
		write(appRoot.resolve("version/assets/classes.txt"));
		Path target = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyProjectForReadOnlyUser(appRoot, target);

		assertTrue(Files.isRegularFile(target.resolve("version/assets/java.md")));
		assertTrue(Files.isRegularFile(target.resolve("version/assets/classes.txt")));
	}

	@Test
	void nothingIsCopiedForAProjectWithNoAppRoot() throws IOException {
		Path target = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyProjectForReadOnlyUser(this.folder.resolve("missing"), target);

		assertFalse(Files.exists(target));
	}

	@Test
	void nobodyCanWriteToAViewersCopy() throws IOException {
		Path appRoot = project();
		Path copy = this.folder.resolve("chroot/app_root");

		SymlinkHelper.copyForViewer(appRoot, copy);

		for (Path part : new Path[] { copy, copy.resolve("version/assets"), copy.resolve("version/assets/py/script.py") }) {
			Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(part);
			assertFalse(permissions.contains(PosixFilePermission.OWNER_WRITE), part.toString());
			assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE), part.toString());
			assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE), part.toString());
			assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ), part.toString());
		}
		assertThrows(AccessDeniedException.class,
				() -> Files.write(copy.resolve("version/assets/py/script.py"), new byte[] { 1 }));
		assertFalse(Files.exists(copy.resolve("version/assets/java")));
		SymlinkHelper.makeWritable(copy);
	}

	@Test
	void aViewersCopyIsMadeAgainInTheSameFolder() throws IOException {
		Path appRoot = project();
		Path copy = this.folder.resolve("chroot/app_root");
		SymlinkHelper.copyForViewer(appRoot, copy);
		Object folderBefore = Files.readAttributes(copy, BasicFileAttributes.class).fileKey();

		Files.delete(appRoot.resolve("version/assets/py/script.py"));
		write(appRoot.resolve("version/assets/py/added.py"));
		SymlinkHelper.copyForViewer(appRoot, copy);

		assertFalse(Files.exists(copy.resolve("version/assets/py/script.py")));
		assertTrue(Files.isRegularFile(copy.resolve("version/assets/py/added.py")));
		assertEquals(folderBefore,
				Files.readAttributes(copy, BasicFileAttributes.class).fileKey());
		SymlinkHelper.makeWritable(copy);
	}

	@Test
	void someoneWhoWasAnEditorGetsACopyInPlaceOfTheLink() throws IOException {
		Path appRoot = project();
		Path inChroot = this.folder.resolve("chroot/app_root");
		Files.createDirectories(inChroot.getParent());
		Files.createSymbolicLink(inChroot, appRoot);

		SymlinkHelper.copyForViewer(appRoot, inChroot);

		assertFalse(Files.isSymbolicLink(inChroot));
		assertTrue(Files.isDirectory(inChroot, LinkOption.NOFOLLOW_LINKS));
		assertTrue(Files.isRegularFile(inChroot.resolve("version/assets/py/script.py")));
		assertFalse(Files.exists(inChroot.resolve("version/assets/java")));
		assertTrue(Files.isRegularFile(appRoot.resolve("version/assets/java/Reactor.java")));
		SymlinkHelper.makeWritable(inChroot);
	}

	@Test
	void someoneWhoBecomesAnEditorLosesTheCopySoTheProjectCanBeLinked() throws IOException {
		Path appRoot = project();
		Path inChroot = this.folder.resolve("chroot/app_root");
		SymlinkHelper.copyForViewer(appRoot, inChroot);

		SymlinkHelper.removeCopy(inChroot);

		assertFalse(Files.exists(inChroot, LinkOption.NOFOLLOW_LINKS));
		assertTrue(Files.isRegularFile(appRoot.resolve("version/assets/py/script.py")));
	}

	@Test
	void removingACopyLeavesAnEditorsLinkAndTheProjectAlone() throws IOException {
		Path appRoot = project();
		Path inChroot = this.folder.resolve("chroot/app_root");
		Files.createDirectories(inChroot.getParent());
		Files.createSymbolicLink(inChroot, appRoot);

		SymlinkHelper.removeCopy(inChroot);

		assertTrue(Files.isSymbolicLink(inChroot));
		assertTrue(Files.isRegularFile(appRoot.resolve("version/assets/py/script.py")));
	}

	@Test
	void aChangedProjectIsToldFromTheOneThatWasCopied() throws IOException {
		Path appRoot = project();
		SymlinkHelper.ProjectFiles copied = SymlinkHelper.ProjectFiles.of(appRoot);
		assertEquals(copied, SymlinkHelper.ProjectFiles.of(appRoot));

		write(appRoot.resolve("version/assets/py/added.py"));
		SymlinkHelper.ProjectFiles withAFileAdded = SymlinkHelper.ProjectFiles.of(appRoot);
		assertNotEquals(copied, withAFileAdded);

		Path script = appRoot.resolve("version/assets/py/script.py");
		Files.setLastModifiedTime(script, FileTime.fromMillis(Files.getLastModifiedTime(script).toMillis() + 60_000));
		assertNotEquals(withAFileAdded, SymlinkHelper.ProjectFiles.of(appRoot));
	}

	@Test
	void aChangeToAFolderTheCopyLeavesOutDoesNotChangeTheCopy() throws IOException {
		Path appRoot = project();
		SymlinkHelper.ProjectFiles copied = SymlinkHelper.ProjectFiles.of(appRoot);

		write(appRoot.resolve("version/assets/java/Another.java"));
		write(appRoot.resolve("version/assets/classes/Another.class"));
		write(appRoot.resolve("version/assets/portals/index.html"));
		write(appRoot.resolve("version/.git/objects/ab/cdef"));

		assertEquals(copied, SymlinkHelper.ProjectFiles.of(appRoot));
	}

	@ParameterizedTest
	@ValueSource(strings = { "project/Orders__1234-abcd/app_root", "project/Orders__1234-abcd/app_root/version",
			"project/Orders__1234-abcd/app_root/version/assets/py",
			"project/Orders__1234-abcd/app_root/version/insight-id",
			"project/My__Orders__1234-abcd/app_root/version/assets" })
	void aFolderOfAProjectIsTracedToTheProject(String inProject) {
		Path base = this.folder.resolve("semosshome");

		SymlinkHelper.ProjectFolder project = SymlinkHelper.ProjectFolder.of(base.resolve(inProject), base);

		assertEquals("1234-abcd", project.projectId());
		assertTrue(project.appRoot().endsWith("app_root"));
		assertTrue(base.resolve(inProject).startsWith(project.appRoot()));
	}

	@ParameterizedTest
	@ValueSource(strings = { "user/Asset__1234-abcd/app_root", "project/Orders__1234-abcd",
			"project/Orders__1234-abcd/other", "project", "InsightCache/abc", "project/NoId/app_root" })
	void aFolderOutsideAProjectsAppRootBelongsToNoProject(String elsewhere) {
		Path base = this.folder.resolve("semosshome");

		assertNull(SymlinkHelper.ProjectFolder.of(base.resolve(elsewhere), base));
	}

	@Test
	void aChangeOfPermissionSwitchesBetweenTheCopyAndTheLink() throws IOException {
		Path base = Files.createDirectories(this.folder.toRealPath().resolve("semosshome"));
		Path appRoot = project();
		Path chroot = this.folder.toRealPath().resolve("chroot-user");
		Path inChroot = chroot.resolve(appRoot.toRealPath().toString().substring(1));
		User user = new User();
		Properties saved = DIHelper.getInstance().getCoreProp();
		Properties settings = new Properties();
		settings.setProperty(Constants.BASE_FOLDER, base.toString());
		settings.setProperty(Constants.INSIGHT_CACHE_DIR,
				Files.createDirectories(base.resolve("InsightCache")).toString());
		Files.createDirectories(base.resolve(Constants.PY_BASE_FOLDER));
		DIHelper.getInstance().setCoreProp(settings);
		try (MockedStatic<ChrootTemplate> template = Mockito.mockStatic(ChrootTemplate.class);
				MockedStatic<AssetUtility> assets = Mockito.mockStatic(AssetUtility.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			assets.when(() -> AssetUtility.getProjectAppRootFolder(PROJECT_ID))
					.thenReturn(appRoot.toRealPath().toString());
			SymlinkHelper helper = new SymlinkHelper(chroot.toString(), user);

			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID)).thenReturn(false);
			helper.symlinkProject(user, PROJECT_ID);
			assertTrue(Files.isDirectory(inChroot, LinkOption.NOFOLLOW_LINKS));
			assertFalse(Files.isSymbolicLink(inChroot));
			assertTrue(Files.isRegularFile(inChroot.resolve("version/assets/py/script.py")));
			assertFalse(Files.exists(inChroot.resolve("version/assets/java")));
			assertFalse(Files.exists(inChroot.resolve("version/.git")));

			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID)).thenReturn(true);
			helper.symlinkProject(user, PROJECT_ID);
			assertTrue(Files.isSymbolicLink(inChroot));
			assertTrue(Files.isRegularFile(inChroot.resolve("version/assets/java/Reactor.java")));

			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID)).thenReturn(false);
			helper.symlinkFolder(appRoot.toRealPath().resolve("version/assets/py").toString());
			assertFalse(Files.isSymbolicLink(inChroot));
			assertTrue(Files.isRegularFile(inChroot.resolve("version/assets/py/script.py")));
			assertFalse(Files.exists(inChroot.resolve("version/assets/java")));
			assertTrue(Files.isRegularFile(appRoot.resolve("version/assets/java/Reactor.java")));

			helper.removeChrootFolder();
			assertFalse(Files.exists(chroot));
			assertTrue(Files.isRegularFile(appRoot.resolve("version/assets/py/script.py")));
		} finally {
			DIHelper.getInstance().setCoreProp(saved);
		}
	}

	@Test
	void aProjectFolderThatCannotBePutInTheChrootIsLoggedAndSkipped() throws IOException {
		Path base = Files.createDirectories(this.folder.toRealPath().resolve("semosshome"));
		Path appRoot = project();
		Path chroot = this.folder.toRealPath().resolve("chroot-user");
		User user = new User();
		Properties saved = DIHelper.getInstance().getCoreProp();
		Properties settings = new Properties();
		settings.setProperty(Constants.BASE_FOLDER, base.toString());
		settings.setProperty(Constants.INSIGHT_CACHE_DIR,
				Files.createDirectories(base.resolve("InsightCache")).toString());
		Files.createDirectories(base.resolve(Constants.PY_BASE_FOLDER));
		DIHelper.getInstance().setCoreProp(settings);
		try (MockedStatic<ChrootTemplate> template = Mockito.mockStatic(ChrootTemplate.class);
				MockedStatic<AssetUtility> assets = Mockito.mockStatic(AssetUtility.class);
				MockedStatic<SecurityProjectUtils> security = Mockito.mockStatic(SecurityProjectUtils.class)) {
			assets.when(() -> AssetUtility.getProjectAppRootFolder(PROJECT_ID))
					.thenReturn(appRoot.toRealPath().toString());
			security.when(() -> SecurityProjectUtils.userCanEditProject(user, PROJECT_ID))
					.thenThrow(new IllegalStateException("The security database could not be read"));
			SymlinkHelper helper = new SymlinkHelper(chroot.toString(), user);

			String pyFolder = appRoot.toRealPath().resolve("version/assets/py").toString();
			assertDoesNotThrow(() -> helper.symlinkFolder(pyFolder));
			assertThrows(IllegalStateException.class, () -> helper.symlinkProject(user, PROJECT_ID));
		} finally {
			DIHelper.getInstance().setCoreProp(saved);
		}
	}

	@Test
	void theInsightCacheEveryUserSharesIsNeverSentToAnOpenShellSandbox() throws IOException {
		Path base = Files.createDirectories(this.folder.toRealPath().resolve("semosshome"));
		Path cache = Files.createDirectories(base.resolve("InsightCache"));
		Path insightFolder = Files.createDirectories(cache.resolve("session-id/insight-id"));
		Files.createDirectories(base.resolve(Constants.PY_BASE_FOLDER));
		Path chroot = this.folder.toRealPath().resolve("chroot-user");
		Properties saved = DIHelper.getInstance().getCoreProp();
		Properties settings = new Properties();
		settings.setProperty(Constants.BASE_FOLDER, base.toString());
		settings.setProperty(Constants.INSIGHT_CACHE_DIR, cache.toString());
		settings.setProperty(Constants.SANDBOX_MODE, Constants.SANDBOX_MODE_OPENSHELL);
		DIHelper.getInstance().setCoreProp(settings);
		List<String> sent = new ArrayList<>();
		try (MockedStatic<ChrootTemplate> template = Mockito.mockStatic(ChrootTemplate.class)) {
			SymlinkHelper helper = new SymlinkHelper(chroot.toString(), new User());
			helper.symlinkFolder(base.toString());
			helper.symlinkFolder(insightFolder.toString());
			// folders asked for before the sandbox exists are sent when it starts
			helper.setTransport(new SandboxTransporter() {
				@Override
				public boolean transport(String absPath) {
					return transport(absPath, false);
				}

				@Override
				public boolean transport(String absPath, boolean viewOnly) {
					return sent.add(absPath);
				}

				@Override
				public boolean close() {
					return true;
				}
			});

			assertFalse(sent.contains(cache.toString()), "the whole insight cache was sent");
			assertFalse(sent.contains(base.toString()), "a folder that holds the insight cache was sent");
			assertTrue(sent.contains(insightFolder.toString()), "a granted insight folder was not sent");
		} finally {
			DIHelper.getInstance().setCoreProp(saved);
		}
	}

	private Path project() throws IOException {
		Path appRoot = this.folder.resolve("semosshome/project/Orders__" + PROJECT_ID + "/app_root");
		write(appRoot.resolve("version/assets/py/script.py"));
		write(appRoot.resolve("version/assets/public/logo.txt"));
		write(appRoot.resolve("version/assets/java/Reactor.java"));
		write(appRoot.resolve("version/assets/classes/Reactor.class"));
		write(appRoot.resolve("version/assets/portals/index.html"));
		write(appRoot.resolve("version/.git/HEAD"));
		return appRoot;
	}

	private static void write(Path file) throws IOException {
		Files.createDirectories(file.getParent());
		Files.write(file, file.getFileName().toString().getBytes(StandardCharsets.UTF_8));
	}

}
