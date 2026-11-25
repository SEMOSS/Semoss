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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.FileMode;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.RemoteRefUpdate;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilterGroup;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.project.api.IProject;
import prerna.reactor.project.BuildAndPublishAppReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.ValueUtils;

/** App Builder checkout and immutable inputs for candidate regression replay. */
public final class AppTestCandidate implements AutoCloseable {
    private static final Logger LOGGER = LogManager.getLogger(AppTestCandidate.class);
    static final String BASELINE_REF = "refs/semoss/app-test-baseline";
    private static final String BRANCH_PREFIX = "app-candidate/";
    private static final String TEST_DIRECTORY = "assets/app-tests/tests";
    private static final String FIXTURE_FILE = "assets/app-tests/database.json";
    private static final Gson JSON = new Gson();

    record Baseline(ObjectId commit, AppTestEnvironment.Definition database, List<Map<String, Object>> recordings) { }

    private final Path repository;
    private final Path checkout;
    private final String projectId;
    private final String branch;
    private final Baseline baseline;

    private AppTestCandidate(Path repository, Path checkout, String projectId, String branch, Baseline baseline) {
        this.repository = repository;
        this.checkout = checkout;
        this.projectId = projectId;
        this.branch = branch;
        this.baseline = baseline;
    }

    /** A project owner approves the committed fixture and recordings for future candidate runs. */
    static Map<String, Object> approve(User actor, String projectId, String sha) throws IOException, SQLException {
        Path path = versionFolder(projectId);
        if (!SecurityProjectUtils.userIsOwner(actor, projectId)) {
            throw new IllegalArgumentException("Only the app owner can change its candidate baseline");
        }
        requireSingleNode();
        try (Git git = Git.open(path.toFile())) {
            Repository repo = git.getRepository();
            RefUpdate update = repo.updateRef(BASELINE_REF);
            ObjectId current = update.getOldObjectId();
            update.setExpectedOldObjectId(current == null ? ObjectId.zeroId() : current);
            if (sha == null) {
                if (current != null) {
                    update.setForceUpdate(true);
                    requireRefUpdate(update.delete());
                }
                return Map.of("target", projectId, "candidateEnabled", false);
            }
            Baseline approved = readBaseline(repo, objectId(sha));
            AppTestEnvironment.authorize(actor, approved.database());
            AppTestEnvironment.checkFixture(approved.database());
            update.setNewObjectId(approved.commit());
            requireRefUpdate(update.forceUpdate());
            return Map.of("target", projectId, "candidateEnabled", true, "baselineCommitSha", sha,
                    "recordings", approved.recordings().size());
        }
    }

    private static void requireRefUpdate(RefUpdate.Result result) throws IOException {
        if (!List.of(RefUpdate.Result.NEW, RefUpdate.Result.NO_CHANGE, RefUpdate.Result.FORCED,
                RefUpdate.Result.FAST_FORWARD).contains(result)) {
            throw new IOException("Could not update the candidate baseline: " + result);
        }
    }

    /** Returns null for apps where the owner has not enabled candidate runs. */
    public static AppTestCandidate open(User actor, String projectId, String gitFolder)
            throws IOException, GitAPIException {
        Path path = Path.of(gitFolder);
        if (!Files.exists(path.resolve(".git"))) return null;
        try (Git git = Git.open(path.toFile())) {
            Repository repo = git.getRepository();
            var approved = repo.exactRef(BASELINE_REF);
            if (approved == null) return null;
            requireSingleNode();
            Baseline baseline = readBaseline(repo, approved.getObjectId());
            AppTestEnvironment.authorize(actor, baseline.database());
            String branch = BRANCH_PREFIX + UUID.randomUUID();
            Path checkout = Files.createTempDirectory("semoss-app-candidate-").toRealPath();
            boolean ready = false;
            try {
                checkOut(repo, checkout, branch, baseline.commit());
                ready = true;
                return new AppTestCandidate(path, checkout, projectId, branch, baseline);
            } finally {
                if (!ready) remove(checkout);
            }
        }
    }

    static void checkOut(Repository source, Path directory, String branch, ObjectId baseline)
            throws IOException, GitAPIException {
        String ref = "refs/heads/" + branch;
        try (Git candidate = Git.init().setDirectory(directory.toFile()).call()) {
            candidate.fetch().setRemote(source.getDirectory().toURI().toString())
                    .setRefSpecs(new RefSpec(BASELINE_REF + ":" + ref)).call();
            if (!baseline.equals(candidate.getRepository().resolve(ref))) {
                throw new IOException("The approved baseline changed while the candidate was opening. Start the run again.");
            }
            candidate.checkout().setName(branch).call();
        }
    }

    public Path checkout() { return checkout; }
    public String branch() { return branch; }

    /** Persist only the candidate branch in the app repository and return the new commit. */
    public String commit(User actor, Path workingDirectory) throws IOException, GitAPIException {
        try (Git git = Git.open(checkout.toFile()); Git app = Git.open(repository.toFile())) {
            String ref = "refs/heads/" + branch;
            if (!ref.equals(git.getRepository().getFullBranch())) {
                throw new IOException("The candidate checkout changed branches unexpectedly");
            }
            String workPath = checkout.relativize(workingDirectory).toString().replace('\\', '/');
            var status = git.status().call();
            var modified = status.getUncommittedChanges().stream()
                    .filter(path -> !isAgentState(path, workPath)).toList();
            var untracked = status.getUntracked().stream().filter(path -> !isAgentState(path, workPath)).toList();
            if (modified.isEmpty() && untracked.isEmpty()) return null;
            if (!modified.isEmpty()) {
                var tracked = git.add().setUpdate(true);
                modified.forEach(tracked::addFilepattern);
                tracked.call();
            }
            if (!untracked.isEmpty()) {
                var added = git.add();
                untracked.forEach(added::addFilepattern);
                added.call();
            }
            var login = actor.getAccessToken(actor.getPrimaryLogin());
            String name = ValueUtils.trimToNull(login.getResolvedUsername());
            String email = ValueUtils.trimToNull(login.getEmail());
            if (name == null) name = "SEMOSS";
            if (email == null) email = "semoss@semoss.org";
            String sha = git.commit().setMessage("App Builder candidate")
                    .setAuthor(name, email).setCommitter(name, email)
                    .call().name();
            var updates = git.push().setRemote(app.getRepository().getDirectory().toURI().toString())
                    .setRefSpecs(new RefSpec(ref + ":" + ref)).call();
            for (var push : updates) {
                var update = push.getRemoteUpdate(ref);
                if (update != null && List.of(RemoteRefUpdate.Status.OK, RemoteRefUpdate.Status.UP_TO_DATE).contains(update.getStatus())) {
                    return sha;
                }
            }
            throw new IOException("Could not save the candidate branch in the app repository");
        }
    }

    private static boolean isAgentState(String path, String workPath) {
        return path.equals(workPath + "/todos.json") || path.startsWith(workPath + "/.claude/skills/");
    }

    public String test(User actor, String sha) throws IOException, URISyntaxException {
        return start(actor, projectId, repository, baseline, sha, branch, 1);
    }

    static String rerun(User actor, String projectId, String sha, int repeat) throws IOException, URISyntaxException {
        requireSingleNode();
        Path path = versionFolder(projectId);
        try (Git git = Git.open(path.toFile())) {
            Repository repo = git.getRepository();
            var approved = repo.exactRef(BASELINE_REF);
            if (approved == null) throw new IllegalArgumentException("Approve an app test baseline first");
            return start(actor, projectId, path, readBaseline(repo, approved.getObjectId()), sha, null, repeat);
        }
    }

    private static String start(User actor, String projectId, Path repository, Baseline baseline, String sha,
            String branch, int repeat) throws IOException, URISyntaxException {
        if (!SecurityProjectUtils.userCanEditProject(actor, projectId)) {
            throw new IllegalArgumentException("App edit access is required to run a candidate regression");
        }
        AppTestEnvironment.authorize(actor, baseline.database());
        Path snapshot = Files.createTempDirectory("semoss-candidate-build-");
        AppTestEnvironment.Portal portal;
        try (Git git = Git.open(repository.toFile()); RevWalk revisions = new RevWalk(git.getRepository())) {
            RevCommit candidate = revisions.parseCommit(objectId(sha));
            if (!revisions.isMergedInto(revisions.parseCommit(baseline.commit()), candidate)) {
                throw new IllegalArgumentException("The candidate must descend from the approved test baseline");
            }
            exportBuildInputs(git.getRepository(), candidate, snapshot);
            Path assets = snapshot.resolve("assets");
            BuildAndPublishAppReactor.buildCandidatePortal(assets);
            portal = AppTestEnvironment.portal(assets.resolve("portals"));
        } finally {
            remove(snapshot);
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("The candidate build was interrupted before replay could start");
        }
        Map<String, Object> provenance = new LinkedHashMap<>();
        provenance.put("candidateCommitSha", sha);
        provenance.put("baselineCommitSha", baseline.commit().name());
        if (branch != null) provenance.put("candidateBranch", branch);
        return AppTestRuns.startCandidate(AppTestRuns.ownerOf(actor), projectId, repeat, baseline.recordings(),
                baseline.database(), portal, provenance);
    }

    static Baseline readBaseline(Repository repo, ObjectId id) throws IOException {
        try (RevWalk revisions = new RevWalk(repo)) {
            RevCommit commit = revisions.parseCommit(id);
            Map<String, Object> fixture;
            try (TreeWalk file = TreeWalk.forPath(repo, FIXTURE_FILE, commit.getTree())) {
                if (file == null) throw new IOException("Commit the enabled H2 fixture at " + FIXTURE_FILE);
                fixture = readJson(file);
            }
            AppTestEnvironment.Definition database = AppTestEnvironment.validate(fixture);
            if (database == null) throw new IOException("The approved baseline needs an enabled H2 fixture");
            List<Map<String, Object>> recordings = new ArrayList<>();
            try (TreeWalk files = new TreeWalk(repo)) {
                files.addTree(commit.getTree());
                files.setRecursive(true);
                files.setFilter(PathFilterGroup.createFromStrings(List.of(TEST_DIRECTORY)));
                while (files.next()) {
                    String path = files.getPathString();
                    if (!path.startsWith(TEST_DIRECTORY + "/") || !path.endsWith(".json")) continue;
                    if (recordings.size() >= AppTestRecordings.MAX_RECORDINGS) throw new IOException("Too many approved recordings");
                    Map<String, Object> recording = AppTestRecordings.validateStored(
                            readJson(files), path.substring(TEST_DIRECTORY.length() + 1));
                    var steps = (List<?>) recording.get("steps");
                    if (steps.stream().noneMatch(step -> ((Map<?, ?>) step).get("type").toString().startsWith("ASSERT_"))) {
                        throw new IOException("Each approved recording needs an outcome check: " + recording.get("name"));
                    }
                    recordings.add(recording);
                }
            }
            if (recordings.isEmpty()) throw new IOException("Commit at least one saved recording with an outcome check");
            return new Baseline(commit.copy(), database, List.copyOf(recordings));
        }
    }

    private static Map<String, Object> readJson(TreeWalk file) throws IOException {
        if (!FileMode.REGULAR_FILE.equals(file.getFileMode(0))) {
            throw new IOException("Expected a regular approved test file: " + file.getPathString());
        }
        byte[] bytes = file.getObjectReader().open(file.getObjectId(0)).getBytes(256_000);
        return JSON.fromJson(new String(bytes, StandardCharsets.UTF_8), new TypeToken<Map<String, Object>>() { }.getType());
    }

    static void exportBuildInputs(Repository repo, RevCommit commit, Path snapshot) throws IOException {
        try (TreeWalk files = new TreeWalk(repo)) {
            files.addTree(commit.getTree());
            files.setRecursive(true);
            files.setFilter(PathFilterGroup.createFromStrings(List.of("assets/client", "assets/portals", "assets/public")));
            while (files.next()) {
                String path = files.getPathString();
                if (path.startsWith("assets/public/") && !path.endsWith(".json")) continue;
                if (!List.of(FileMode.REGULAR_FILE, FileMode.EXECUTABLE_FILE).contains(files.getFileMode(0))) {
                    throw new IOException("Candidate build inputs must be regular Git files: " + path);
                }
                Path output = snapshot.resolve(path).normalize();
                if (!output.startsWith(snapshot.resolve("assets")) || path.indexOf('\\') >= 0) {
                    throw new IOException("Invalid candidate build path");
                }
                Files.createDirectories(output.getParent());
                try (var input = files.getObjectReader().open(files.getObjectId(0)).openStream()) {
                    Files.copy(input, output);
                }
            }
        }
    }

    private static ObjectId objectId(String sha) {
        if (sha == null || !sha.matches("[a-f0-9]{40}")) {
            throw new IllegalArgumentException("Provide a full lowercase Git commit SHA");
        }
        return ObjectId.fromString(sha);
    }

    private static Path versionFolder(String projectId) {
        IProject project = Utility.getProject(projectId);
        if (project == null || project.getProjectType() != IProject.PROJECT_TYPE.CODE) {
            throw new IllegalArgumentException("Candidate regression requires a CODE app");
        }
        return Path.of(AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId));
    }

    private static void requireSingleNode() {
        if (ClusterUtil.IS_CLUSTER) throw new IllegalArgumentException("Candidate branches currently require a single SEMOSS server");
    }

    @Override
    public void close() { remove(checkout); }

    private static void remove(Path directory) {
        try {
            FileUtils.deleteDirectory(directory.toFile());
        } catch (IOException failure) {
            LOGGER.warn("Could not remove a temporary app candidate directory", failure);
        }
    }
}
