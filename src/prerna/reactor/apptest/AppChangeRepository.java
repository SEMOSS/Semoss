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
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.MergeCommand;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.treewalk.TreeWalk;

/** Local Git operations for candidate changes. No force checkout, resets of app files or conflict recovery. */
final class AppChangeRepository {
    private AppChangeRepository() { }
    static boolean runtimeOutput(String path) {
        return path.startsWith("assets/app-tests/test-runs/") || path.startsWith("assets/.claude/skills/");
    }
    static String cleanHead(Path folder, String baseBranch) throws Exception {
        try (Git git = Git.open(folder.toFile())) {
            if (!git.getRepository().getBranch().equals(baseBranch))
                throw new IllegalArgumentException("Checkout " + baseBranch + " in the app repository before using test-gated changes");
            if (!git.getRepository().getRepositoryState().canCheckout())
                throw new IllegalArgumentException("Resolve the app repository's pending Git operation first");
            Set<String> dirty = dirty(git);
            if (!dirty.isEmpty()) throw new IllegalArgumentException("Commit the app's existing edits before starting a tested change: " + dirty.stream().sorted().limit(5).toList());
            ObjectId head = git.getRepository().resolve(Constants.HEAD);
            if (head == null) throw new IllegalArgumentException("The app needs an initial commit");
            return head.name();
        }
    }
    static Set<String> dirty(Git git) throws Exception {
        var status = git.status().call();
        Set<String> paths = new HashSet<>(status.getUncommittedChanges());
        paths.addAll(status.getUntracked());
        paths.addAll(status.getConflicting());
        paths.removeIf(AppChangeRepository::runtimeOutput);
        return paths;
    }
    static void create(Path source, Path candidate, String base, String branch, String expected) throws Exception {
        try (Git git = Git.open(source.toFile())) {
            if (!expected.equals(cleanHead(source, base))) throw new IllegalArgumentException("Base branch changed before checkout creation");
            GitRepoSafety.checkTree(git, ObjectId.fromString(expected));
            git.branchCreate().setName(branch).setStartPoint(expected).call();
        }
        try (Git git = Git.cloneRepository().setURI(source.toUri().toString()).setDirectory(candidate.toFile()).setBranch(branch).call()) {
            // No default push destination: publication belongs to the coordinator, not the agent.
            git.remoteRemove().setRemoteName("origin").call();
            // Prevent staged system skills and test reports from becoming application changes.
            Files.createDirectories(git.getRepository().getDirectory().toPath().resolve("info"));
            Files.writeString(git.getRepository().getDirectory().toPath().resolve("info/exclude"),
                    "\n/assets/.claude/skills/\n/assets/app-tests/test-runs/\n", java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }
    }
    static String head(Path folder) throws Exception {
        try (Git git = Git.open(folder.toFile())) { return git.getRepository().resolve(Constants.HEAD).name(); }
    }
    static String commit(Path candidate, String branch, PersonIdent author, AppTestEnvironment.Portal portal) throws Exception {
        try (Git git = Git.open(candidate.toFile())) {
            if (!branch.equals(git.getRepository().getBranch())) throw new IllegalArgumentException("The candidate branch changed unexpectedly");
            if (!dirty(git).isEmpty() || portal != null) {
                git.add().addFilepattern(".").call();
                git.add().setUpdate(true).addFilepattern(".").call();
                git.reset().addPath("assets/app-tests/test-runs").addPath("assets/.claude/skills").setRef(Constants.HEAD).call();
                if(portal!=null) stagePortal(git,portal);
                if(git.diff().setCached(true).call().isEmpty())return git.getRepository().resolve(Constants.HEAD).name();
                git.commit().setMessage("Update app on " + branch).setAuthor(author).setCommitter(author).call();
            }
            ObjectId head = git.getRepository().resolve(Constants.HEAD);
            GitRepoSafety.checkTree(git, head);
            return head.name();
        }
    }
    private static void stagePortal(Git git,AppTestEnvironment.Portal portal) throws Exception {
        Map<String,byte[]> files=new java.util.TreeMap<>();
        portal.assets().forEach((name,bytes)->files.put("assets/portals/"+name,bytes));
        portal.publicJson().forEach((name,text)->files.put("assets/"+name,text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        Path assets=git.getRepository().getWorkTree().toPath().resolve("assets");
        for(var test:AppTestRecordings.list(assets.resolve("app-tests"))) {
            String path="assets/app-tests/tests/"+AppTestRecordings.id((String)test.get("id"))+".json";
            files.put(path,Files.readAllBytes(git.getRepository().getWorkTree().toPath().resolve(path)));
        }
        Path database=assets.resolve("app-tests/database.json");
        if(Files.exists(database,LinkOption.NOFOLLOW_LINKS)) {
            if(!Files.isRegularFile(database,LinkOption.NOFOLLOW_LINKS)||Files.size(database)>256_000)throw new IOException("Invalid candidate database configuration");
            files.put("assets/app-tests/database.json",Files.readAllBytes(database));
        }
        var index=git.getRepository().lockDirCache();
        try(var objects=git.getRepository().newObjectInserter()) {
            var editor=index.editor();
            for(var file:files.entrySet()) {
                ObjectId blob=objects.insert(Constants.OBJ_BLOB,file.getValue());
                editor.add(new org.eclipse.jgit.dircache.DirCacheEditor.PathEdit(file.getKey()) {
                    @Override public void apply(org.eclipse.jgit.dircache.DirCacheEntry entry) {
                        entry.setFileMode(org.eclipse.jgit.lib.FileMode.REGULAR_FILE);entry.setObjectId(blob);entry.setLength(file.getValue().length);
                    }
                });
            }
            objects.flush();if(!editor.commit())throw new IOException("Could not stage the candidate build");
        } finally {index.unlock();}
    }
    static void retainBranch(Path source, Path candidate, String branch) throws Exception {
        try (Git git = Git.open(source.toFile())) {
            var result = git.fetch().setRemote(candidate.toUri().toString())
                    .setRefSpecs(new RefSpec("refs/heads/"+branch+":refs/heads/"+branch)).call();
            for (var update : result.getTrackingRefUpdates())
                if (!Set.of(org.eclipse.jgit.lib.RefUpdate.Result.NEW, org.eclipse.jgit.lib.RefUpdate.Result.FAST_FORWARD,
                        org.eclipse.jgit.lib.RefUpdate.Result.NO_CHANGE).contains(update.getResult()))
                    throw new IllegalArgumentException("Could not preserve candidate branch: " + update.getResult());
        }
    }
    static void merge(Path source, Path candidate, String base, String branch, String expectedBase, String testedCommit) throws Exception {
        if (!testedCommit.equals(head(candidate))) throw new IllegalArgumentException("The candidate changed after testing");
        try (Git git = Git.open(candidate.toFile())) {
            if (!dirty(git).isEmpty()) throw new IllegalArgumentException("Candidate files changed after testing; run tests again");
        }
        if (!expectedBase.equals(cleanHead(source, base))) throw new IllegalArgumentException("The base branch moved; preserve this candidate and retest against the new base");
        retainBranch(source, candidate, branch);
        try (Git git = Git.open(source.toFile())) {
            if (!testedCommit.equals(git.getRepository().resolve("refs/heads/"+branch).name())) throw new IllegalArgumentException("Candidate branch no longer matches tested commit");
            var result = git.merge().include(ObjectId.fromString(testedCommit)).setFastForward(MergeCommand.FastForwardMode.FF_ONLY).call();
            if (!Set.of(org.eclipse.jgit.api.MergeResult.MergeStatus.FAST_FORWARD, org.eclipse.jgit.api.MergeResult.MergeStatus.ALREADY_UP_TO_DATE).contains(result.getMergeStatus()))
                throw new IllegalArgumentException("The tested change could not fast-forward: " + result.getMergeStatus());
        }
    }
    /** Reject symlinks/submodules before materializing an agent checkout. */
    private static final class GitRepoSafety {
        static void checkTree(Git git, ObjectId commit) throws IOException {
            try (var walk = new org.eclipse.jgit.revwalk.RevWalk(git.getRepository()); var tree = new TreeWalk(git.getRepository())) {
                tree.addTree(walk.parseCommit(commit).getTree()); tree.setRecursive(true);
                while (tree.next()) if (tree.getFileMode(0).equals(org.eclipse.jgit.lib.FileMode.SYMLINK)
                        || tree.getFileMode(0).equals(org.eclipse.jgit.lib.FileMode.GITLINK))
                    throw new IllegalArgumentException("Test-gated changes require one app repository without symlinks/submodules: " + tree.getPathString());
            }
        }
    }
    static void trackedPortal(Path candidate, AppTestEnvironment.Portal portal) throws Exception {
        try (Git git = Git.open(candidate.toFile()); var walk = new org.eclipse.jgit.revwalk.RevWalk(git.getRepository())) {
            var commit = walk.parseCommit(git.getRepository().resolve(Constants.HEAD));
            Map<String,byte[]> inputs=new java.util.TreeMap<>();
            portal.assets().forEach((name,bytes)->inputs.put("assets/portals/"+name,bytes));
            portal.publicJson().forEach((name,text)->inputs.put("assets/"+name,text.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            for (var asset : inputs.entrySet()) {
                try (var entry = TreeWalk.forPath(git.getRepository(), asset.getKey(), commit.getTree())) {
                    if (entry == null || !java.util.Arrays.equals(asset.getValue(), git.getRepository().open(entry.getObjectId(0)).getBytes()))
                        throw new IllegalArgumentException("The built portal must be tracked in the tested commit: assets/portals/"+asset.getKey());
                }
            }
        }
    }
}
