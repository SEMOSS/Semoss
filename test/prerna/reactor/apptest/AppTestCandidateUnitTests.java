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
package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.revwalk.RevWalk;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.project.api.IProject;
import prerna.reactor.project.BuildAndPublishAppReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

class AppTestCandidateUnitTests {
    private static final String ENGINE = "source";
    private static final String RECORDING_ID = "94e82b66-61ce-449c-b6c2-b7e1dcbcfb71";
    @TempDir Path directory;

    @Test
    void commitsOnlyToCandidateAndUsesTheApprovedFixtureAndRecording() throws Exception {
        Path app = Files.createDirectory(directory.resolve("app"));
        try (Git live = Git.init().setDirectory(app.toFile()).setInitialBranch("main").call()) {
            ObjectId approved = baseline(live, app);
            assertNull(AppTestCandidate.open(null, "app", app.toString()));
            approve(live, approved);
            Files.writeString(app.resolve("assets/portals/index.html"), "Uncommitted live content");
            User actor = mock(User.class);
            AccessToken login = mock(AccessToken.class);
            when(actor.getPrimaryLogin()).thenReturn(AuthProvider.NATIVE);
            when(actor.getAccessToken(AuthProvider.NATIVE)).thenReturn(login);
            when(login.getResolvedUsername()).thenReturn("App editor");
            when(login.getEmail()).thenReturn("editor@example.com");
            String branch;
            String candidateSha;
            try (var engines = mockStatic(SecurityEngineUtils.class)) {
                engines.when(() -> SecurityEngineUtils.userCanViewEngine(actor, ENGINE)).thenReturn(true);
                engines.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype(ENGINE))
                        .thenReturn(new Object[] { IEngine.CATALOG_TYPE.DATABASE, RdbmsTypeEnum.H2_DB.getLabel() });
                AppTestCandidate candidate = AppTestCandidate.open(actor, "app", app.toString());
                assertNotNull(candidate);
                Path checkout = candidate.checkout();
                try (candidate) {
                    branch = candidate.branch();
                    Path assets = checkout.resolve("assets");
                    assertEquals("<p>Baseline portal</p>", Files.readString(assets.resolve("portals/index.html")));
                    Files.writeString(assets.resolve("todos.json"), "{}");
                    assertNull(candidate.commit(actor, assets));
                    Files.writeString(assets.resolve("portals/index.html"), "<p>Candidate portal</p>");
                    Files.writeString(assets.resolve("portals/style.css"), "p { color: black; }");
                    Files.delete(assets.resolve("app-tests/tests/" + RECORDING_ID + ".json"));
                    Files.delete(assets.resolve("app-tests/database.json"));
                    candidateSha = candidate.commit(actor, assets);
                }
                assertFalse(Files.exists(checkout));
            }

            assertEquals(approved, live.getRepository().resolve("HEAD"));
            assertEquals("main", live.getRepository().getBranch());
            assertEquals("Uncommitted live content", Files.readString(app.resolve("assets/portals/index.html")));
            assertEquals(candidateSha, live.getRepository().resolve("refs/heads/" + branch).name());
            assertFalse(Files.exists(app.resolve("assets/portals/style.css")));

            var frozen = AppTestCandidate.readBaseline(live.getRepository(), approved);
            assertEquals(List.of(RECORDING_ID), frozen.recordings().stream().map(test -> test.get("id")).toList());
            assertEquals(ENGINE, frozen.database().sourceEngineId());
            assertThrows(IOException.class,
                    () -> AppTestCandidate.readBaseline(live.getRepository(), ObjectId.fromString(candidateSha)));

            Path snapshot = Files.createDirectory(directory.resolve("snapshot"));
            try (RevWalk revisions = new RevWalk(live.getRepository())) {
                AppTestCandidate.exportBuildInputs(live.getRepository(),
                        revisions.parseCommit(ObjectId.fromString(candidateSha)), snapshot);
            }
            BuildAndPublishAppReactor.buildCandidatePortal(snapshot.resolve("assets"));
            assertEquals("<p>Candidate portal</p>", Files.readString(snapshot.resolve("assets/portals/index.html")));
            assertEquals("p { color: black; }", Files.readString(snapshot.resolve("assets/portals/style.css")));
        }
    }

    @Test
    void theOwnerCanApproveAndClearTheBaselineWithoutChangingTheBranch() throws Exception {
        Path app = Files.createDirectory(directory.resolve("app"));
        User actor = mock(User.class);
        IProject project = mock(IProject.class);
        when(project.getProjectName()).thenReturn("Candidate app");
        when(project.getProjectType()).thenReturn(IProject.PROJECT_TYPE.CODE);
        try (Git live = Git.init().setDirectory(app.toFile()).call();
                var projects = mockStatic(SecurityProjectUtils.class);
                var engines = mockStatic(SecurityEngineUtils.class);
                var utility = mockStatic(Utility.class);
                var paths = mockStatic(AssetUtility.class)) {
            ObjectId approved = baseline(live, app);
            utility.when(() -> Utility.getProject("app")).thenReturn(project);
            paths.when(() -> AssetUtility.getProjectVersionFolder("Candidate app", "app")).thenReturn(app.toString());
            engines.when(() -> SecurityEngineUtils.userCanViewEngine(actor, ENGINE)).thenReturn(true);
            engines.when(() -> SecurityEngineUtils.getEngineTypeAndSubtype(ENGINE))
                    .thenReturn(new Object[] { IEngine.CATALOG_TYPE.DATABASE, RdbmsTypeEnum.H2_DB.getLabel() });
            assertThrows(IllegalArgumentException.class, () -> AppTestCandidate.approve(actor, "app", approved.name()));
            projects.when(() -> SecurityProjectUtils.userIsOwner(actor, "app")).thenReturn(true);
            assertEquals(approved.name(), AppTestCandidate.approve(actor, "app", approved.name()).get("baselineCommitSha"));
            assertEquals(approved, live.getRepository().resolve(AppTestCandidate.BASELINE_REF));
            assertEquals(false, AppTestCandidate.approve(actor, "app", null).get("candidateEnabled"));
            assertNull(live.getRepository().exactRef(AppTestCandidate.BASELINE_REF));
            assertEquals(approved, live.getRepository().resolve("HEAD"));
        }
    }

    @Test
    void rejectsAChangedBaselineWhileOpeningTheCheckout() throws Exception {
        Path app = Files.createDirectory(directory.resolve("app"));
        try (Git live = Git.init().setDirectory(app.toFile()).call()) {
            ObjectId original = baseline(live, app);
            Files.writeString(app.resolve("assets/portals/index.html"), "<p>New baseline</p>");
            live.add().addFilepattern("assets/portals/index.html").call();
            approve(live, live.commit().setMessage("New baseline").setAuthor("Owner", "owner@example.com").call());
            Path checkout = Files.createDirectory(directory.resolve("checkout"));
            assertThrows(IOException.class,
                    () -> AppTestCandidate.checkOut(live.getRepository(), checkout, "app-candidate/test", original));
        }
    }

    @Test
    void keepsTheCandidateAndBaselineIdentityInHistory() {
        Map<String, Object> run = Map.of("mode", "ISOLATED_H2", "candidateBranch", "app-candidate/test",
                "candidateCommitSha", "a".repeat(40), "baselineCommitSha", "b".repeat(40), "tests", List.of());
        assertEquals(run, AppTestHistory.compact(run));
    }

    private static ObjectId baseline(Git git, Path app) throws Exception {
        Path assets = app.resolve("assets");
        Files.createDirectories(assets.resolve("app-tests/tests"));
        Files.createDirectories(assets.resolve("portals"));
        Files.writeString(assets.resolve("portals/index.html"), "<p>Baseline portal</p>");
        Files.writeString(assets.resolve("app-tests/database.json"), """
                {"enabled":true,"sourceEngineId":"source","schemaSql":"CREATE TABLE TASKS (ID INT);","seedSql":""}
                """);
        Files.writeString(assets.resolve("app-tests/tests/" + RECORDING_ID + ".json"), """
                {"version":1,"id":"%s","name":"Load task","description":"Check the task is visible",
                 "startPath":"","steps":[{"type":"ASSERT_VISIBLE","selector":"p"}]}
                """.formatted(RECORDING_ID));
        git.add().addFilepattern("assets").call();
        return git.commit().setMessage("Approved app").setAuthor("Owner", "owner@example.com").call().copy();
    }

    private static void approve(Git git, ObjectId sha) throws IOException {
        var approval = git.getRepository().updateRef(AppTestCandidate.BASELINE_REF);
        approval.setNewObjectId(sha);
        assertEquals(RefUpdate.Result.NEW, approval.update());
    }
}
