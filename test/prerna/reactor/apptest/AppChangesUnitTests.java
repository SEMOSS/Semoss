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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.PersonIdent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.project.api.IProject;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunTarget;
import prerna.util.AppChangeScope;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Real temporary Git repositories and real Chromium; no application project or production database. */
class AppChangesUnitTests {
    @TempDir Path temporary;
    private org.mockito.MockedStatic<prerna.auth.utils.SecurityProjectUtils> access;
    @org.junit.jupiter.api.BeforeEach void allowAppEditing() {
        access=mockStatic(prerna.auth.utils.SecurityProjectUtils.class);
        access.when(()->prerna.auth.utils.SecurityProjectUtils.userCanEditProject(any(User.class),eq("app"))).thenReturn(true);
    }
    @org.junit.jupiter.api.AfterEach void closeAccessMock() { access.close(); }
    private final PersonIdent author=new PersonIdent("Test editor","editor@semoss.invalid");
    private Path repository(String name, boolean tests) throws Exception {
        Path version=temporary.resolve(name).resolve("app_root/version");Files.createDirectories(version.resolve("assets/portals"));
        Files.writeString(version.resolve("assets/portals/index.html"),"<html><body><div data-testid='state'>Before</div></body></html>");
        if(tests)AppTestRecordings.save(version.resolve("assets/app-tests"),Map.of("name","Expected state","description","Check the requested state", "startPath","","steps",List.of(Map.of("type","ASSERT_TEXT","selector","[data-testid=\"state\"]","text","After","exact",true))));
        try(Git git=Git.init().setInitialBranch("master").setDirectory(version.toFile()).call()) {
            git.add().addFilepattern(".").call();git.commit().setMessage("Initial app").setAuthor(author).setCommitter(author).call();
        }
        return version;
    }
    private void enable(Path source) throws Exception {
        AppTestRecordings.write(source.getParent().resolve("app-tests/changes/configuration.json"),Map.of("enabled",true,"baseBranch","master","buildCommand",""));
    }
    private User user(String id,AuthProvider provider) {
        User user=mock(User.class);AccessToken token=mock(AccessToken.class);
        when(user.getPrimaryLogin()).thenReturn(provider);when(user.getAccessToken(provider)).thenReturn(token);
        when(token.getId()).thenReturn(id);when(token.getName()).thenReturn("Editor");when(token.getEmail()).thenReturn("editor@semoss.invalid");
        return user;
    }
    private IProject project() {
        IProject project=mock(IProject.class);when(project.getProjectId()).thenReturn("app");when(project.getProjectName()).thenReturn("App");return project;
    }
    private AgentRunTarget target(Path source,IProject project) {return AgentRunTarget.project(project,source.resolve("assets").toString(),source.toString());}
    private final AgentHarnessResult completed=new AgentHarnessResult("Candidate ready",1,List.of());

    @Test void passingCandidateBuildIsTestedAndMergedWithoutExposingEditsEarly() throws Exception {
        Path source=repository("passing",true);enable(source);String original=AppChangeRepository.head(source);var project=project();
        try(var utility=mockStatic(Utility.class,CALLS_REAL_METHODS)) {
            utility.when(()->Utility.getProject("app")).thenReturn(project);
            utility.when(Utility::getApplicationUrl).thenReturn("http://semoss-app-test.invalid/Monolith");
            utility.when(Utility::getPublicHomeFolder).thenReturn("public_home");
            try(var session=AppChanges.begin(target(source,project),user("editor",AuthProvider.NATIVE),"semoss")) {
                Path candidate=Path.of(session.target().getGitFolder());
                assertNotEquals(source,candidate);
                assertTrue(Path.of(session.target().getWorkingDirectory()).toRealPath().startsWith(candidate.toRealPath().resolve("assets")));
                assertEquals(candidate.resolve("assets").toString().replace('\\','/'),AssetUtility.getProjectAssetsFolder("App","app"));
                Files.writeString(candidate.resolve("assets/portals/index.html"),"<html><body><div data-testid='state'>After</div></body></html>");
                assertTrue(Files.readString(source.resolve("assets/portals/index.html")).contains("Before"));
                assertEquals(original,AppChangeRepository.head(source));
                String outcome=session.finish(completed);
                assertTrue(outcome.contains("merged into master"),outcome);
            }
            assertNull(AppChangeScope.current());assertFalse(AppChanges.active("app"));
            assertNotEquals(original,AppChangeRepository.head(source));
            assertTrue(Files.readString(source.resolve("assets/portals/index.html")).contains("After"));verify(project).setRepublish(true);
        }
    }
    @Test void agentRunnerRoutesEditsAndPublishesTheActualGateOutcome() throws Exception {
        Path source=repository("agent-runner",true);enable(source);var project=project();var actor=user("editor",AuthProvider.NATIVE);
        var insight=mock(prerna.om.Insight.class);when(insight.getUser()).thenReturn(actor);when(insight.getInsightFolder()).thenReturn("original-room-folder");
        var room=mock(prerna.engine.impl.model.Room.class);when(room.getId()).thenReturn("room");when(room.getUserId()).thenReturn("editor");when(room.getModelId()).thenReturn("model");when(room.getOptionsMap()).thenReturn(new java.util.HashMap<>());
        var model=mock(prerna.engine.api.IModelEngine.class);var config=mock(prerna.reactor.agent.config.AgentConfig.class);
        when(config.getSkills()).thenReturn(List.of());when(config.getRunHooks()).thenReturn(List.of());
        var harness=mock(prerna.reactor.agent.IAgentHarness.class);when(harness.getName()).thenReturn("semoss");
        when(harness.execute(any())).thenAnswer(invocation->{
            prerna.reactor.agent.AgentRunContext context=invocation.getArgument(0);
            assertTrue(context.getInput().contains("PLATFORM TEST-GATED APP CHANGE"));
            Path assets=Path.of(context.getAgentTarget().getWorkingDirectory());assertNotEquals(source.resolve("assets"),assets);
            assertEquals(assets.toString(),AssetUtility.getProjectAssetsFolder("App","app"));
            var publish=new prerna.reactor.project.PublishProjectReactor();publish.In();publish.setInsight(insight);
            var projectArgument=new prerna.sablecc2.om.GenRowStruct();projectArgument.add(new prerna.sablecc2.om.nounmeta.NounMetadata("app",prerna.sablecc2.om.PixelDataType.CONST_STRING));publish.getNounStore().addNoun("project",projectArgument);
            assertTrue(publish.execute().getValue().toString().contains("Candidate saved"));verify(project,never()).setRepublish(true);
            Files.writeString(assets.resolve("portals/index.html"),"<html><body><div data-testid='state'>After</div></body></html>");
            // Finish the coding turn without polling its diagnostic run: the coordinator must drain it before the final gate.
            AppTestRuns.start("NATIVE:editor","app",null,null,List.of(),null,null,null,1,null,true,AppTestEnvironment.candidateDefinition(actor,"app"),AppTestBrowserOptions.DEFAULT);
            return completed;
        });
        when(room.appendHarnessResponse(anyString(),nullable(String.class),any(),any())).thenAnswer(invocation->{
            var message=prerna.engine.impl.model.message.ResponseMessage.text(invocation.getArgument(0));message.setMessageId("platform-result");return message;
        });
        try(var utility=mockStatic(Utility.class,CALLS_REAL_METHODS);
            var rooms=mockStatic(prerna.engine.impl.model.RoomUtils.class);
            var modelAccess=mockStatic(prerna.auth.utils.SecurityEngineUtils.class);
            var modelMetadata=mockStatic(prerna.auth.utils.SecurityModelMetadataUtils.class);
            var engines=mockStatic(prerna.util.EngineUtility.class);
            var assets=mockStatic(AssetUtility.class,CALLS_REAL_METHODS);
            var loader=mockStatic(prerna.reactor.agent.config.AgentConfigLoader.class);
            var registry=mockStatic(prerna.reactor.agent.AgentHarnessRegistry.class);
            var skills=mockStatic(prerna.reactor.agent.skill.SkillStager.class);
            var namer=mockStatic(prerna.reactor.agent.run.AgentRoomNamer.class)) {
            utility.when(()->Utility.getProject("app")).thenReturn(project);utility.when(()->Utility.getModel("model")).thenReturn(model);utility.when(Utility::getApplicationUrl).thenReturn("http://semoss-app-test.invalid/Monolith");utility.when(Utility::getPublicHomeFolder).thenReturn("public_home");
            rooms.when(()->prerna.engine.impl.model.RoomUtils.getOrLoadRoom("room",insight)).thenReturn(room);
            modelAccess.when(()->prerna.auth.utils.SecurityEngineUtils.userCanViewEngine(actor,"model")).thenReturn(true);modelAccess.when(()->prerna.auth.utils.SecurityEngineUtils.getEngineType("model")).thenReturn(prerna.engine.api.IEngine.CATALOG_TYPE.MODEL);

            engines.when(()->prerna.util.EngineUtility.getSpecificEngineVersionFolder(prerna.engine.api.IEngine.CATALOG_TYPE.PROJECT,"app","App")).thenReturn(source.toString());
            assets.when(()->AssetUtility.getProjectAppRootFolder("App","app")).thenAnswer(invocation->AppChangeScope.current("app")==null?source.getParent().toString():AppChangeScope.current("app").appRoot.toString());
            loader.when(()->prerna.reactor.agent.config.AgentConfigLoader.load(any(),any(),anyString(),anyMap(),anyMap(),anyInt(),anyInt(),any())).thenReturn(config);
            registry.when(()->prerna.reactor.agent.AgentHarnessRegistry.getOrDefault("semoss")).thenReturn(harness);
            var result=prerna.reactor.agent.AgentRunner.run("room","Change the state",null,"semoss",10,0,Map.of("space","app"),Map.of(),null,insight);
            assertTrue(result.getFinalText().contains("merged into master"),result.getFinalText());assertEquals("platform-result",result.getFinalOutputMessageId());
            assertTrue(Files.readString(source.resolve("assets/portals/index.html")).contains("After"));verify(insight).setInsightFolder("original-room-folder");
            var evidence=(Map<?,?>)AppChanges.testResult("app").get("run");assertEquals("COMPLETED",evidence.get("status"));assertEquals(AppChangeRepository.head(source),evidence.get("candidateCommit"));
            assertFalse(((Map<?,?>)((List<?>)evidence.get("tests")).getFirst()).containsKey("evidence"));
            assertTrue(Files.isRegularFile(source.resolve("assets/app-tests/test-runs").resolve(evidence.get("runId")+".json")));
            Path archive=source.getParent().resolve("app-tests/changes").resolve((String)evidence.get("changeId")).resolve("test-runs");
            try(var reports=Files.list(archive)){assertEquals(2,reports.count(),"Diagnostic and final gate evidence must survive successful checkout cleanup");}
        }
    }
    @Test void failingCandidateRemainsOnItsBranchAndNextChatCanRepairIt() throws Exception {
        Path source=repository("repair",true);enable(source);String original=AppChangeRepository.head(source);var project=project();
        try(var utility=mockStatic(Utility.class,CALLS_REAL_METHODS)) {
            utility.when(()->Utility.getProject("app")).thenReturn(project);utility.when(Utility::getApplicationUrl).thenReturn("http://semoss-app-test.invalid/Monolith");utility.when(Utility::getPublicHomeFolder).thenReturn("public_home");
            Path candidate;
            try(var session=AppChanges.begin(target(source,project),user("editor",AuthProvider.NATIVE),"semoss")) {
                candidate=Path.of(session.target().getGitFolder());Files.writeString(candidate.resolve("assets/portals/index.html"),"<html><body><div data-testid='state'>Wrong</div></body></html>");
                assertTrue(session.finish(completed).contains("not merged"));
            }
            assertEquals(original,AppChangeRepository.head(source));
            assertThrows(IllegalArgumentException.class,()->AppChanges.begin(target(source,project),user("editor",AuthProvider.MICROSOFT),"semoss"));
            try(var session=AppChanges.begin(target(source,project),user("editor",AuthProvider.NATIVE),"semoss")) {
                assertEquals(candidate,Path.of(session.target().getGitFolder()));Files.writeString(candidate.resolve("assets/portals/index.html"),"<html><body><div data-testid='state'>After</div></body></html>");
                String outcome=session.finish(completed);assertTrue(outcome.contains("merged into master"),outcome);
            }
        }
    }
    @Test void changingRequiredTestsOrMissingCoverageCannotMerge() throws Exception {
        Path source=repository("weaken",true);enable(source);String original=AppChangeRepository.head(source);
        try(var session=AppChanges.begin(target(source,project()),user("editor",AuthProvider.NATIVE),"semoss")) {
            Path assets=Path.of(session.target().getWorkingDirectory());
            try(var tests=Files.list(assets.resolve("app-tests/tests"))){Files.delete(tests.findFirst().orElseThrow());}
            assertTrue(session.finish(completed).contains("Existing regression test was changed or removed"));
        }
        assertEquals(original,AppChangeRepository.head(source));
        Path empty=repository("no-coverage",false);enable(empty);String head=AppChangeRepository.head(empty);
        try(var session=AppChanges.begin(target(empty,project()),user("editor",AuthProvider.NATIVE),"semoss")) {
            Files.writeString(Path.of(session.target().getWorkingDirectory()).resolve("portals/index.html"),"Changed");
            assertTrue(session.finish(completed).contains("Add a saved workflow"));
        }
        assertEquals(head,AppChangeRepository.head(empty));
    }
    @Test void dirtyBaseAndConcurrentBaseAdvanceArePreserved() throws Exception {
        Path source=repository("dirty",false);Files.writeString(source.resolve("assets/user-edit.txt"),"Keep my edit");
        assertThrows(IllegalArgumentException.class,()->AppChangeRepository.cleanHead(source,"master"));assertEquals("Keep my edit",Files.readString(source.resolve("assets/user-edit.txt")));
        Path clean=repository("advance",false);String base=AppChangeRepository.head(clean);Path candidate=temporary.resolve("candidate");String branch="app-change/"+UUID.randomUUID();AppChangeRepository.create(clean,candidate,"master",branch,base);
        Files.writeString(candidate.resolve("assets/portals/index.html"),"Candidate");String tested=AppChangeRepository.commit(candidate,branch,author,null);
        Files.writeString(clean.resolve("assets/another-edit.txt"),"Concurrent edit");
        String advanced;
        try(Git git=Git.open(clean.toFile())) {git.add().addFilepattern(".").call();advanced=git.commit().setMessage("Concurrent app edit").setAuthor(author).setCommitter(author).call().name();}
        assertThrows(IllegalArgumentException.class,()->AppChangeRepository.merge(clean,candidate,"master",branch,base,tested));assertEquals(advanced,AppChangeRepository.head(clean));
        assertTrue(Files.readString(clean.resolve("assets/portals/index.html")).contains("Before"));
    }
    @Test void editsAfterTestingInvalidateTheCandidateAndIgnoredBuildOutputsAreCommitted() throws Exception {
        Path source=repository("outputs",false);String base=AppChangeRepository.head(source);Path candidate=temporary.resolve("output-candidate");String branch="app-change/"+UUID.randomUUID();AppChangeRepository.create(source,candidate,"master",branch,base);
        Files.writeString(candidate.resolve(".gitignore"),"assets/portals/new.js\n");Files.writeString(candidate.resolve("assets/portals/new.js"),"window.tested=true;");
        var portal=AppTestEnvironment.portal(candidate.resolve("assets/portals"));String tested=AppChangeRepository.commit(candidate,branch,author,portal);AppChangeRepository.trackedPortal(candidate,portal);
        Files.writeString(candidate.resolve("assets/portals/index.html"),"Unverified later edit");
        assertThrows(IllegalArgumentException.class,()->AppChangeRepository.merge(source,candidate,"master",branch,base,tested));assertEquals(base,AppChangeRepository.head(source));
    }
    @Test void approvalRequiredToolsAreDeniedInsteadOfEscapingTheCandidateLease() throws Exception {
        var type=Class.forName("prerna.reactor.agent.runtime.HarnessToolExecutor");
        var parsedType=Class.forName(type.getName()+"$ParsedToolCall");
        var constructor=parsedType.getDeclaredConstructor(Map.class);constructor.setAccessible(true);
        Map<String,Object> call=Map.of("id","ask","name","UnsafeTool","arguments",Map.of(),"_meta",Map.of("SMSS_MCP_EXECUTION","ask"));
        Object parsed=constructor.newInstance(call);
        var asks=type.getDeclaredMethod("getAskToolCalls",List.class);asks.setAccessible(true);
        var execute=type.getDeclaredMethod("executeToolSafely",parsedType,prerna.reactor.agent.AgentRunContext.class,Map.class,String.class,java.util.concurrent.atomic.AtomicInteger.class);execute.setAccessible(true);
        try(var scope=AppChangeScope.enter(new AppChangeScope.Context("app","NATIVE:editor","change",temporary))) {
            assertTrue(((List<?>)asks.invoke(null,List.of(call))).isEmpty());
            Object denied=execute.invoke(null,parsed,null,Map.of(),null,new java.util.concurrent.atomic.AtomicInteger());
            var success=denied.getClass().getDeclaredField("success");success.setAccessible(true);assertEquals(false,success.get(denied));
        }
        assertTrue(AppChanges.supportsMcpTool("RunAppTests"));assertFalse(AppChanges.supportsMcpTool("GitHubPushProject"));
    }
    @Test void oldOrIncompleteOrSmokeEvidenceIsRejected() {
        String id=UUID.randomUUID().toString();Map<String,Object> test=new LinkedHashMap<>(Map.of("id",id,"definitionHash","definition","status","PASSED","assertions",1,"assertionsPassed",1,"steps",2,"stepsCompleted",2,"scope","SAVED_WORKFLOW_REGRESSION"));
        Map<String,Object> report=new LinkedHashMap<>(Map.of("target","app","status","COMPLETED","changeId","change","candidateCommit","commit","portalHash","portal","tests",List.of(test)));
        Map<String,String> expected=Map.of(id,"definition");assertDoesNotThrow(()->AppChanges.requirePass(report,"app","change","commit","portal",expected));
        assertThrows(IllegalArgumentException.class,()->AppChanges.requirePass(report,"app","change","old-commit","portal",expected));
        for(String status:List.of("SKIPPED","FAILED","ERROR")){test.put("status",status);assertThrows(IllegalArgumentException.class,()->AppChanges.requirePass(report,"app","change","commit","portal",expected));}
        test.put("status","PASSED");test.put("scope","PAGE_VISIBILITY_SMOKE");assertThrows(IllegalArgumentException.class,()->AppChanges.requirePass(report,"app","change","commit","portal",expected));
        test.put("scope","SAVED_WORKFLOW_REGRESSION");test.put("stepsCompleted",1);assertThrows(IllegalArgumentException.class,()->AppChanges.requirePass(report,"app","change","commit","portal",expected));
    }
    @Test void parallelToolContextRetainsTheCandidateAndRestoresTheWorkerThread() throws Exception {
        var candidate=new AppChangeScope.Context("app","NATIVE:editor","change",temporary);
        var type=Class.forName("prerna.reactor.agent.runtime.HarnessToolExecutor$AgentThreadContext");
        var capture=type.getDeclaredMethod("capture");capture.setAccessible(true);var call=type.getDeclaredMethod("call",java.util.function.Supplier.class);call.setAccessible(true);
        try(var scope=AppChangeScope.enter(candidate);var pool=java.util.concurrent.Executors.newSingleThreadExecutor()) {
            Object context=capture.invoke(null);
            assertEquals("change",pool.submit(()->{
                Object value=call.invoke(context,(java.util.function.Supplier<String>)()->AppChangeScope.current("app").changeId);
                assertNull(AppChangeScope.current());return value;
            }).get(5,java.util.concurrent.TimeUnit.SECONDS));
            assertSame(candidate,AppChangeScope.current());
        }
    }
    @Test void ignoredExistingBuildFilesCanBePromotedOnlyAfterTesting() throws Exception {
        Path source=repository("ignored-build",false);
        Files.writeString(source.resolve(".gitignore"),"assets/portals/\n");Files.writeString(source.resolve("assets/source.txt"),"Source app");
        try(Git git=Git.open(source.toFile())) {
            git.rm().setCached(true).addFilepattern("assets/portals/index.html").call();git.add().addFilepattern(".").call();git.commit().setMessage("Ignore generated portal").setAuthor(author).setCommitter(author).call();
        }
        String base=AppChangeRepository.head(source),branch="app-change/"+UUID.randomUUID();Path candidate=temporary.resolve("ignored-candidate");AppChangeRepository.create(source,candidate,"master",branch,base);
        Files.createDirectories(candidate.resolve("assets/portals"));Files.writeString(candidate.resolve("assets/portals/index.html"),"<html><body>Tested build</body></html>");
        var portal=AppTestEnvironment.portal(candidate.resolve("assets/portals"));String commit=AppChangeRepository.commit(candidate,branch,author,portal);
        AppChangeRepository.merge(source,candidate,"master",branch,base,commit);
        assertTrue(Files.readString(source.resolve("assets/portals/index.html")).contains("Tested build"));
    }
    @Test void malformedWorkflowSettingsDoNotSilentlyDisableTheGate() throws Exception {
        Path source=repository("bad-settings",false);String original=AppChangeRepository.head(source);
        AppTestRecordings.write(source.getParent().resolve("app-tests/changes/configuration.json"),Map.of("enabled","true"));
        assertThrows(IllegalArgumentException.class,()->AppChanges.begin(target(source,project()),user("editor",AuthProvider.NATIVE),"semoss"));
        assertEquals(original,AppChangeRepository.head(source));assertFalse(AppChanges.active("app"));
    }
    @Test void failedBuildCannotBeTreatedAsAReadyCandidateAndWritesOnlyBoundedPrivateLog() throws Exception {
        Path assets=temporary.resolve("build-assets");Files.createDirectories(assets);Path log=temporary.resolve("private/build.log");
        assertThrows(IllegalArgumentException.class,()->AppChanges.build(assets,"echo build-failed; exit 7",log));assertTrue(Files.readString(log).contains("build-failed"));
        Files.writeString(assets.resolve("package.json"),"{\"scripts\":{\"build\":\"vite build\"}}");assertThrows(IllegalArgumentException.class,()->AppChanges.build(assets,"",log));
    }
    @Test void oneAppChangeAtATimeAndScopeIsAlwaysRestored() throws Exception {
        Path source=repository("lock",false);enable(source);var project=project();
        try(var session=AppChanges.begin(target(source,project),user("editor",AuthProvider.NATIVE),"semoss")) {
            assertThrows(IllegalArgumentException.class,()->AppChanges.begin(target(source,project),user("editor",AuthProvider.NATIVE),"semoss"));
            assertNotNull(AppChangeScope.current("app"));assertNull(AppChangeScope.current("other-app"));
        }
        assertFalse(AppChanges.active("app"));assertNull(AppChangeScope.current());
    }
}
