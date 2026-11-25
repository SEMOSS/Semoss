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

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.eclipse.jgit.lib.PersonIdent;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunTarget;
import prerna.util.AppChangeScope;
import prerna.util.AssetUtility;
import prerna.util.Utility;

/** Owns opt-in, local app change sessions. Only this coordinator may authorize a tested fast-forward. */
public final class AppChanges {
    private static final Gson JSON = new Gson();
    private static final Set<String> ACTIVE = ConcurrentHashMap.newKeySet();
    private static final Set<String> TERMINAL = Set.of("MERGED", "NO_CHANGES", "ABANDONED");
    private AppChanges() { }
    /** Only candidate-aware in-process app tools may operate beside the native file/build tools. */
    public static boolean supportsMcpTool(String name) {
        return Set.of("RunAppTests","GetAppTestRun","ListAppTestRecordings","GetAppTestRecording","SaveAppTestRecording",
                "DeleteAppTestRecording","DeleteInvalidAppTestFile","ListAppTestRuns","GetAppTestRunHistory",
                "ManageAppTestDatabase","ManageAppTestChanges","ProjectGitStatus","ProjectGitBranches","ProjectGitDiff").contains(name);
    }
    public static boolean active(String project) { return ACTIVE.contains(project); }

    private static Path storage(String project) {
        if (AppChangeScope.current() != null) throw new IllegalArgumentException("Change workflow settings cannot be edited from a candidate run");
        return Path.of(AssetUtility.getProjectAppRootFolder(project), "app-tests", "changes");
    }
    private static Map<String,Object> read(Path path) throws Exception {
        if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) return new LinkedHashMap<>();
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path)>256_000) throw new IllegalArgumentException("Invalid change workflow metadata");
        Map<String,Object> value = JSON.fromJson(Files.readString(path), new TypeToken<Map<String,Object>>(){}.getType());
        if (value == null) throw new IllegalArgumentException("Invalid change workflow metadata");
        return value;
    }
    static Map<String,Object> configuration(Path store) throws Exception {
        Map<String,Object> value = read(store.resolve("configuration.json"));
        value.putIfAbsent("enabled", false); value.putIfAbsent("baseBranch", "master"); value.putIfAbsent("buildCommand", "");
        if(!(value.get("enabled") instanceof Boolean) || !(value.get("baseBranch") instanceof String branch)
                || !org.eclipse.jgit.lib.Repository.isValidRefName("refs/heads/"+branch)
                || !(value.get("buildCommand") instanceof String command) || command.length()>2000
                || value.containsKey("version") && (!(value.get("version") instanceof Number version) || version.intValue()!=1))
            throw new IllegalArgumentException("Invalid app change workflow configuration; refusing to bypass the merge gate");
        return value;
    }
    public static Map<String,Object> get(String project) throws Exception {
        try(var ignored=AppChangeScope.enter(null)) {
            Path store = storage(project);
            return Map.of("target", project, "configuration", configuration(store), "change", publicState(read(store.resolve("latest.json"))), "isActive", active(project));
        }
    }
    public static Map<String,Object> buildLog(String project,String owner) throws Exception {
        try(var ignored=AppChangeScope.enter(null)) {
            Path store=storage(project);var state=read(store.resolve("latest.json"));
            if(!owner.equals(state.get("owner")))throw new IllegalArgumentException("Only the change owner can read its build log");
            Path path=store.resolve(AppTestRecordings.id((String)state.get("id"))).resolve("build.log");
            if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS))return Map.of("target",project,"log", "No build log is available");
            if(Files.size(path)>65_536)throw new IllegalArgumentException("Invalid build log size");
            return Map.of("target",project,"log",Files.readString(path));
        }
    }
    public static Map<String,Object> testResult(String project) throws Exception {
        try(var ignored=AppChangeScope.enter(null)) {
            Path store=storage(project);
            var report=read(store.resolve("last-test-result.json"));
            if(!project.equals(report.get("target")) || !(report.get("candidateCommit") instanceof String commit) || !commit.matches("[0-9a-f]{40}")
                    || !(report.get("changeId") instanceof String change))throw new IllegalArgumentException("No completed candidate evidence is available");
            AppTestRecordings.id(change);
            return Map.of("target",project,"run",AppTestHistory.compact(report));
        }
    }
    private static Map<String,Object> publicState(Map<String,Object> state) {
        Map<String,Object> result = new LinkedHashMap<>();
        for (String key : List.of("id", "branch", "baseBranch", "baseCommit", "candidateCommit", "status", "reason", "runId", "portalHash", "startedAt", "updatedAt", "mergedAt", "summary"))
            if (state.containsKey(key)) result.put(key, state.get(key));
        return result;
    }
    public static synchronized Map<String,Object> save(String project, Map<String,Object> values) throws Exception {
        if (active(project)) throw new IllegalArgumentException("Wait for the current app change to finish before changing its workflow settings");
        if (!(values.get("enabled") instanceof Boolean)) throw new IllegalArgumentException("enabled must be true or false");
        if(values.containsKey("baseBranch") && !(values.get("baseBranch") instanceof String) || values.containsKey("buildCommand") && !(values.get("buildCommand") instanceof String))throw new IllegalArgumentException("baseBranch and buildCommand must be strings");
        String branch = String.valueOf(values.getOrDefault("baseBranch", "master")).trim();
        String command = String.valueOf(values.getOrDefault("buildCommand", "")).trim();
        if (!org.eclipse.jgit.lib.Repository.isValidRefName("refs/heads/"+branch) || branch.equals("HEAD")) throw new IllegalArgumentException("Provide a valid base branch");
        if (command.length()>2000 || command.indexOf('\0')>=0) throw new IllegalArgumentException("Build command is invalid or too long");
        if (Boolean.TRUE.equals(values.get("enabled")) && ClusterUtil.IS_CLUSTER) throw new IllegalArgumentException("Test-gated changes currently require a single SEMOSS server");
        Path store = storage(project);
        AppTestRecordings.write(store.resolve("configuration.json"), Map.of("version",1,"enabled",values.get("enabled"),"baseBranch",branch,"buildCommand",command));
        return get(project);
    }
    public static synchronized Map<String,Object> abandon(String project, String owner) throws Exception {
        if (active(project)) throw new IllegalArgumentException("Stop or wait for the app agent before abandoning its change");
        Path store = storage(project); Map<String,Object> state = read(store.resolve("latest.json"));
        if (!state.isEmpty() && !TERMINAL.contains(state.get("status"))) {
            if (!owner.equals(state.get("owner"))) throw new IllegalArgumentException("Only the change owner can abandon it");
            state.put("status", "ABANDONED"); state.put("reason", "Candidate retained; the next chat starts from the base branch"); persist(store, state);
        }
        return get(project);
    }
    private static void persist(Path store, Map<String,Object> state) throws Exception {
        state.put("updatedAt", Instant.now().toString());
        String id=AppTestRecordings.id((String)state.get("id"));
        AppTestRecordings.write(store.resolve(id+".json"), state);
        AppTestRecordings.write(store.resolve("latest.json"), state);
    }

    /** Runs before config/skill staging so every supported agent file tool sees the candidate root. */
    public static synchronized Session begin(AgentRunTarget target, User user, String harnessType) throws Exception {
        if (!target.isProject()) return null;
        if (AppChangeScope.current()!=null) throw new IllegalArgumentException("Nested agent runs are unavailable during a tested app change");
        if (target.getGitFolder()==null) return null;
        Path store = Path.of(target.getGitFolder()).toAbsolutePath().getParent().resolve("app-tests/changes");
        Map<String,Object> config = configuration(store);
        if (!Boolean.TRUE.equals(config.get("enabled"))) return null;
        if (ClusterUtil.IS_CLUSTER) throw new IllegalArgumentException("Test-gated changes currently require a single SEMOSS server");
        if (harnessType!=null && !harnessType.isBlank() && !harnessType.equals("semoss")) throw new IllegalArgumentException("Test-gated changes currently support the SEMOSS agent harness; external harnesses are not routed to candidates");
        for(String property:List.of("AGENT_DEFAULT_TOOLS_MCP_ID","AGENT_DEFAULT_TOOLS_MCP_PROJECT_ID")) {
            String override=Utility.getDIHelperProperty(property);
            if(override!=null && !override.isBlank())throw new IllegalArgumentException("Test-gated changes currently require the built-in SEMOSS file tools; the legacy default MCP tool provider is not candidate-aware");
        }
        String project=target.getProjectId(), owner=AppTestRuns.ownerOf(user);
        if(AppTestRuns.active(project))throw new IllegalArgumentException("Wait for the current app test/inspection to finish before starting a change");
        if (!ACTIVE.add(project)) throw new IllegalArgumentException("Another tested change is active for this app");
        FileChannel channel=null; FileLock lock=null;
        try {
            Files.createDirectories(store);
            channel=FileChannel.open(store.resolve("active.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
            lock=channel.tryLock(); if(lock==null)throw new IllegalArgumentException("Another tested change is active for this app");
            Path source=Path.of(target.getGitFolder()).toRealPath();
            if(Files.exists(source.resolve("assets/.git"),LinkOption.NOFOLLOW_LINKS))throw new IllegalArgumentException("Nested asset Git repositories are not supported by test-gated changes yet");
            if (!Path.of(target.getWorkingDirectory()).toRealPath().startsWith(source.resolve("assets"))) throw new IllegalArgumentException("Test-gated app chat must run from the app assets folder");
            Map<String,Object> state=read(store.resolve("latest.json"));
            if (!state.isEmpty() && !TERMINAL.contains(state.get("status"))) {
                if (!owner.equals(state.get("owner"))) throw new IllegalArgumentException("This app has an unfinished change owned by another user");
                if (!state.get("baseBranch").equals(config.get("baseBranch"))) throw new IllegalArgumentException("Abandon the pending change before selecting a different base branch");
                state.put("status", "EDITING"); state.remove("reason");
                for(String key:List.of("runId","summary","portalHash","candidateCommit"))state.remove(key);
            } else {
                String base=(String)config.get("baseBranch"), commit=AppChangeRepository.cleanHead(source,base);
                String id=UUID.randomUUID().toString(), branch="app-change/"+id;
                Path candidateRoot=store.resolve(id).resolve("app_root");
                Files.createDirectories(candidateRoot);
                AppChangeRepository.create(source,candidateRoot.resolve("version"),base,branch,commit);
                state=new LinkedHashMap<>(Map.of("id",id,"owner",owner,"branch",branch,"baseBranch",base,"baseCommit",commit,"startedAt",Instant.now().toString(),"status","EDITING"));
                state.put("requiredTests", definitions(candidateRoot.resolve("version/assets/app-tests")));
            }
            Path root=store.resolve(AppTestRecordings.id((String)state.get("id"))).resolve("app_root");
            if (!Files.isDirectory(root.resolve("version/.git"),LinkOption.NOFOLLOW_LINKS)) throw new IllegalArgumentException("Candidate checkout is missing; abandon this change to start again");
            persist(store,state);
            return new Session(target,user,store,source,root,config,state,channel,lock);
        } catch (Exception failure) {
            if(lock!=null)lock.close(); if(channel!=null)channel.close(); ACTIVE.remove(project); throw failure;
        }
    }
    static Map<String,String> definitions(Path tests) throws Exception {
        Map<String,String> hashes=new LinkedHashMap<>();
        for(var test:AppTestRecordings.list(tests)) hashes.put((String)test.get("id"),AppTestHistory.definitionHash(test));
        return hashes;
    }
    static void requirePass(Map<String,Object> report, String project, String change, String commit, String portalHash,
            Map<String,String> expected) {
        if (!"COMPLETED".equals(report.get("status")) || !project.equals(report.get("target")) || !change.equals(report.get("changeId"))
                || !commit.equals(report.get("candidateCommit")) || !portalHash.equals(report.get("portalHash")))
            throw new IllegalArgumentException("Test evidence does not match this candidate commit and built portal");
        if (!(report.get("tests") instanceof List<?> tests) || tests.size()!=expected.size() || tests.isEmpty())
            throw new IllegalArgumentException("No complete workflow coverage for this candidate");
        Set<String> seen=new java.util.HashSet<>();
        for(Object raw:tests) {
            if (!(raw instanceof Map<?,?> test) || !"PASSED".equals(test.get("status")) || !seen.add(String.valueOf(test.get("id")))
                    || !java.util.Objects.equals(expected.get(test.get("id")),test.get("definitionHash"))
                    || !(test.get("assertions") instanceof Number count) || count.intValue()<1
                    || !(test.get("assertionsPassed") instanceof Number passed) || passed.intValue()!=count.intValue()
                    || !(test.get("steps") instanceof Number steps) || !(test.get("stepsCompleted") instanceof Number completed)
                    || steps.intValue()<1 || steps.intValue()!=completed.intValue() || count.intValue()>steps.intValue()
                    || test.get("cleanup") instanceof Map<?,?> cleanup && !"PASSED".equals(cleanup.get("status"))
                    || !"SAVED_WORKFLOW_REGRESSION".equals(test.get("scope")))
                throw new IllegalArgumentException("Every selected workflow and assertion must pass; failed, skipped, smoke-only or incomplete coverage cannot merge");
        }
        if(!seen.equals(expected.keySet()))throw new IllegalArgumentException("Required test identities did not match");
    }

    public static final class Session implements AutoCloseable {
        private final AgentRunTarget original;
        private final User user;
        private final Path store,source,root;
        private final Map<String,Object> config,state;
        private final FileChannel channel;
        private final FileLock lock;
        private final AppChangeScope scope;
        private final AppChangeScope.Context context;
        private boolean finished;
        private Session(AgentRunTarget original,User user,Path store,Path source,Path root,Map<String,Object> config,
                Map<String,Object> state,FileChannel channel,FileLock lock) {
            this.original=original;this.user=user;this.store=store;this.source=source;this.root=root;this.config=config;this.state=state;this.channel=channel;this.lock=lock;
            context=new AppChangeScope.Context(original.getProjectId(),(String)state.get("owner"),(String)state.get("id"),root);
            scope=AppChangeScope.enter(context);
        }
        public AgentRunTarget target() {
            Path assets=root.resolve("version/assets");
            try {
                Path working=Path.of(original.getWorkingDirectory()).toRealPath();
                Path sourceAssets=source.resolve("assets");
                if(!working.startsWith(sourceAssets))throw new IllegalArgumentException("The app working directory left its authorized assets root");
                Path relative=sourceAssets.relativize(working);
                return original.forCandidate(assets.toString(),root.resolve("version").toString()).withWorkingDirectory(assets.resolve(relative).normalize().toString());
            }catch(java.io.IOException failure){throw new IllegalArgumentException("Could not resolve the app working directory",failure);}
        }
        public String instructions() {
            return "\nPLATFORM TEST-GATED APP CHANGE: You are editing private branch "+state.get("branch")+" based on "+state.get("baseBranch")+". All app work must stay in the candidate directory. Do not checkout, merge, publish, edit Git metadata, change test expectations to obtain a pass, or change workflow settings. Install candidate dependencies if needed. The platform will run the configured build and all saved workflows, commit the exact tested files, and fast-forward the base only after successful checks. Use RunAppTests for iterative diagnosis, not as proof that a merge has happened. Add meaningful regression coverage if none exists. Report your changes as a candidate; the platform determines final merge status.";
        }
        public String finish(AgentHarnessResult result) {
            try {
                if(result==null || result.getCompletionError()!=null) throw new IllegalArgumentException("Agent did not finish successfully; candidate retained");
                if(AppTestRuns.active(context.projectId)) {
                    status("WAITING_FOR_TESTS");
                    long until=System.nanoTime()+(AppTestRunner.RUN_TIMEOUT_MS+AppTestBrowserOptions.INSPECTION_MS+15_000)*1_000_000L;
                    while(AppTestRuns.active(context.projectId)) {
                        if(System.nanoTime()>until)throw new IllegalArgumentException("A diagnostic test or inspection is still active; close its window and continue the candidate in chat");
                        Thread.sleep(200); // Interrupted waits retain the candidate and never merge.
                    }
                }
                boolean noChanges;
                try(var git=org.eclipse.jgit.api.Git.open(root.resolve("version").toFile())) {
                    noChanges=AppChangeRepository.dirty(git).isEmpty() && state.get("baseCommit").equals(AppChangeRepository.head(root.resolve("version")));
                }
                if(noChanges) {
                    state.put("status","NO_CHANGES");state.put("reason","No app files changed; nothing merged");persist(store,state);finished=true;return "No app changes to merge.";
                }
                Path version=root.resolve("version");
                var token=user.getAccessToken(user.getPrimaryLogin());
                String author=token.getName()==null || token.getName().isBlank()?context.owner:token.getName();
                String email=token.getEmail()==null || token.getEmail().isBlank()?"app-change@semoss.invalid":token.getEmail();
                state.put("candidateCommit",AppChangeRepository.commit(version,(String)state.get("branch"),new PersonIdent(author,email),null));
                AppChangeRepository.retainBranch(source,version,(String)state.get("branch"));
                status("BUILDING"); build(root.resolve("version/assets"),(String)config.get("buildCommand"),store.resolve(state.get("id").toString()).resolve("build.log"));
                var portal=AppTestEnvironment.portal(version.resolve("assets/portals"));
                String commit=AppChangeRepository.commit(version,(String)state.get("branch"),new PersonIdent(author,email),portal);
                context.commit=commit;state.put("candidateCommit",commit);
                AppChangeRepository.retainBranch(source,version,(String)state.get("branch"));
                Map<String,String> tests=definitions(version.resolve("assets/app-tests"));
                @SuppressWarnings("unchecked") Map<String,String> pinned=(Map<String,String>)state.get("requiredTests");
                for(var entry:pinned.entrySet()) if(!entry.getValue().equals(tests.get(entry.getKey())))throw new IllegalArgumentException("Existing regression test was changed or removed: "+entry.getKey());
                if(tests.isEmpty())throw new IllegalArgumentException("Add a saved workflow with outcome assertions before this change can merge");
                AppChangeRepository.trackedPortal(version,portal); state.put("portalHash",portal.hash());
                status("TESTING");
                var database=AppTestEnvironment.candidateDefinition(user,context.projectId);
                String run=AppTestRuns.start(context.owner,context.projectId,null,null,List.of(),null,null,null,1,null,true,database,AppTestBrowserOptions.DEFAULT);
                state.put("runId",run);persist(store,state);
                long until=System.nanoTime()+180_000_000_000L; Map<String,Object> report;
                do {
                    if(Thread.currentThread().isInterrupted())throw new InterruptedException("App change interrupted");
                    report=AppTestRuns.get(context.owner,run);
                    if(!Set.of("RUNNING","QUEUED").contains(report.get("status")))break;
                    if(System.nanoTime()>until)throw new IllegalArgumentException("Timed out awaiting candidate tests; candidate retained");
                    Thread.sleep(200);
                } while(true);
                state.put("summary",report.getOrDefault("summary",Map.of()));
                AppTestRecordings.write(store.resolve(context.changeId).resolve("test-result.json"),AppTestHistory.compact(report));
                AppTestRecordings.write(store.resolve("last-test-result.json"),AppTestHistory.compact(report));
                requirePass(report,context.projectId,context.changeId,commit,portal.hash(),tests);
                if(!database.fixtureHash().equals(report.get("fixtureHash")) || !database.fixtureHash().equals(AppTestEnvironment.candidateDefinition(user,context.projectId).fixtureHash()))throw new IllegalArgumentException("Test fixture changed after testing");
                if(!portal.hash().equals(AppTestEnvironment.portal(version.resolve("assets/portals")).hash()))throw new IllegalArgumentException("Built portal/settings changed after testing");
                if(!tests.equals(definitions(version.resolve("assets/app-tests"))))throw new IllegalArgumentException("Test definitions changed after testing");
                if(!prerna.auth.utils.SecurityProjectUtils.userCanEditProject(user,context.projectId))throw new IllegalArgumentException("App edit access was revoked; the candidate cannot merge");
                status("MERGING");
                if(Thread.currentThread().isInterrupted())throw new InterruptedException("App change cancelled before merge");
                AppChangeRepository.merge(source,version,(String)state.get("baseBranch"),(String)state.get("branch"),(String)state.get("baseCommit"),commit);
                state.put("status","MERGED"); state.put("mergedAt",Instant.now().toString()); state.put("reason","All candidate workflows passed; base fast-forwarded to the tested commit");finished=true;
                // A merged candidate now matches the normal checkout: publish its result in the existing Testing history.
                Path privateReport=root.resolve("app-tests/reports").resolve(AppTestRuns.ownerDirectory(context.owner)).resolve(run+".json");
                if(Files.isRegularFile(privateReport,LinkOption.NOFOLLOW_LINKS)) {
                    Path destination=source.getParent().resolve("app-tests/reports").resolve(AppTestRuns.ownerDirectory(context.owner)).resolve(run+".json");
                    Files.createDirectories(destination.getParent());Files.copy(privateReport,destination,java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                }
                String warning=AppTestHistory.publishAt(context.projectId,source.resolve("assets/app-tests/test-runs"),report);
                if(warning!=null)state.put("reason",state.get("reason")+". "+warning);
                try(var ignored=AppChangeScope.enter(null)) { Utility.getProject(context.projectId).setRepublish(true); }
                persist(store,state);
                return "App change "+state.get("branch")+" passed all regression workflows and merged into "+state.get("baseBranch")+" ("+commit.substring(0,10)+").";
            } catch(Exception failure) {
                if(failure instanceof InterruptedException)Thread.currentThread().interrupt();
                if("MERGED".equals(state.get("status"))) {
                    state.put("reason","Tested commit merged; history/publishing/status notification needs attention");
                    try{persist(store,state);}catch(Exception ignored){}
                    return "The tested app change merged into "+state.get("baseBranch")+", but its publishing/status notification failed; refresh the app and inspect server logs.";
                }
                fail(failure.getMessage());finished=true;
                return "App change was not merged. "+state.get("reason")+". Candidate branch "+state.get("branch")+" is retained; continue in chat to repair it or abandon it in Testing.";
            }
        }
        private void status(String status) throws Exception { state.put("status",status);persist(store,state); }
        private void fail(String reason) {
            String safe=reason==null?"App change could not finish":reason.replace(root.toString(),"<candidate>").replace(source.toString(),"<app>");
            state.put("status","FAILED");state.put("reason",safe.substring(0,Math.min(1000,safe.length())));
            try {persist(store,state);}catch(Exception problem){org.apache.logging.log4j.LogManager.getLogger(AppChanges.class).error("Could not persist app change {}",context.changeId,problem);}
        }
        @Override public void close() throws Exception {
            try {if(!finished)fail("Agent run stopped before the merge gate completed");}
            finally {
                scope.close();
                if(TERMINAL.contains(state.get("status"))) {
                    try {
                        archiveReports(root.resolve("version/assets/app-tests/test-runs"),store.resolve(context.changeId).resolve("test-runs"),256_000);
                        archiveReports(root.resolve("app-tests/reports").resolve(AppTestRuns.ownerDirectory(context.owner)),store.resolve(context.changeId).resolve("reports"),8_000_000);
                        try(var files=Files.walk(root)) { for(Path path:files.sorted(java.util.Comparator.reverseOrder()).toList())Files.delete(path); }
                    }
                    catch(Exception cleanup){org.apache.logging.log4j.LogManager.getLogger(AppChanges.class).warn("Could not remove completed app candidate {}",context.changeId,cleanup);}
                }
                try{lock.close();}finally{try{channel.close();}finally{ACTIVE.remove(context.projectId);}}
            }
        }
    }
    /** Preserve candidate-only diagnostic attempts before removing a completed private checkout. */
    private static void archiveReports(Path from,Path to,long maximumBytes) throws Exception {
        if(!Files.isDirectory(from,LinkOption.NOFOLLOW_LINKS))return;
        Files.createDirectories(to);
        try(var files=Files.list(from)) {
            for(Path file:files.toList()) {
                String name=file.getFileName().toString();
                if(!name.endsWith(".json") || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))continue;
                AppTestRecordings.id(name.substring(0,name.length()-5));
                if(Files.size(file)>maximumBytes)throw new IllegalArgumentException("Candidate report exceeds archive limit");
                Files.copy(file,to.resolve(name),java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
    static void build(Path assets,String command,Path log) throws Exception {
        if(command.isBlank()) {
            Path manifest=assets.resolve("package.json");
            if(Files.isRegularFile(manifest)) {
                var json=com.google.gson.JsonParser.parseString(Files.readString(manifest)).getAsJsonObject();
                if(json.has("scripts") && json.getAsJsonObject("scripts").has("build"))throw new IllegalArgumentException("Configure a build command for this app before merging source changes");
            }
            return;
        }
        if(Boolean.parseBoolean(Utility.getDIHelperProperty(prerna.util.Constants.STRICT_SCRIPT_SOURCE)))throw new IllegalArgumentException("Candidate shell builds are disabled by the existing strict script source policy");
        boolean windows=System.getProperty("os.name").toLowerCase().contains("win");
        Process process=new ProcessBuilder(windows?List.of("cmd","/c",command):List.of("/bin/sh","-c",command)).directory(assets.toFile()).redirectErrorStream(true).start();
        Files.createDirectories(log.getParent());
        Thread reader=Thread.ofVirtual().start(()->{
            try(var input=process.getInputStream();var destination=Files.newOutputStream(log)) {
                byte[] bytes=new byte[4096];int total=0,n;
                while((n=input.read(bytes))!=-1){int count=Math.min(n,Math.max(0,65_536-total));destination.write(bytes,0,count);total+=count;}
            }catch(java.io.IOException ignored){/* Process termination may close its output. */}
        });
        try {
            if(!process.waitFor(120,TimeUnit.SECONDS))throw new IllegalArgumentException("Candidate build exceeded two minutes");
            reader.join(5000);
            if(reader.isAlive())throw new IllegalArgumentException("Candidate build left a background process holding its output open");
            if(process.exitValue()!=0)throw new IllegalArgumentException("Candidate build failed (exit "+process.exitValue()+"); read its private build log");
        } finally { process.descendants().forEach(ProcessHandle::destroyForcibly);process.destroyForcibly();reader.interrupt(); }

    }
}
