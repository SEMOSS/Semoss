package prerna.util.git;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reproduces the cluster failure where an rclone sync deleted freshly committed
 * objects: HEAD points at a commit that is not in the object database, JGit's
 * status throws "Missing unknown <sha>" wrapped in JGitInternalException, and
 * the user's file operation used to fail even though the file was on disk.
 */
class GitRepoUtilsCorruptRepoTest {

	@TempDir
	Path repoDir;

	private RevCommit initWithCommit() throws Exception {
		try (Git git = Git.init().setDirectory(repoDir.toFile()).call()) {
			Files.writeString(repoDir.resolve("first.txt"), "one");
			git.add().addFilepattern("first.txt").call();
			return git.commit().setMessage("first").setAuthor("Test", "test@test.com").call();
		}
	}

	private void deleteObject(ObjectId id) throws IOException {
		String name = id.getName();
		Path object = repoDir.resolve(".git").resolve("objects").resolve(name.substring(0, 2)).resolve(name.substring(2));
		assertTrue(Files.deleteIfExists(object), "loose object should exist before deletion");
	}

	@Test
	void commitAddedFiles_missingHeadObject_reinitializesAndCommitsWorkingTree() throws Exception {
		RevCommit head = initWithCommit();
		deleteObject(head);
		Files.writeString(repoDir.resolve("second.txt"), "two");

		// sanity: the repository is broken the way production was
		try (Git git = Git.open(repoDir.toFile())) {
			JGitInternalException error = assertThrows(JGitInternalException.class, () -> git.status().call());
			assertTrue(error.getMessage().startsWith("Missing "), error.getMessage());
			assertTrue(GitRepoUtils.isCorruptRepositoryError(error));
		}

		GitRepoUtils.addFilesRelativeToRepo(repoDir.toString(), Arrays.asList("second.txt"));
		assertDoesNotThrow(() -> GitRepoUtils.commitAddedFiles(repoDir.toString(), "save second", "Test", "test@test.com"));

		try (Git git = Git.open(repoDir.toFile())) {
			assertTrue(git.status().call().isClean());
			RevCommit commit = git.log().call().iterator().next();
			assertTrue(commit.getFullMessage().startsWith("save second"), commit.getFullMessage());
			assertTrue(commit.getFullMessage().contains("reinitialized"), commit.getFullMessage());
			assertNotNull(TreeWalk.forPath(git.getRepository(), "first.txt", commit.getTree()));
			assertNotNull(TreeWalk.forPath(git.getRepository(), "second.txt", commit.getTree()));
		}
		// files on disk are never touched
		assertEquals("one", Files.readString(repoDir.resolve("first.txt")));
		assertEquals("two", Files.readString(repoDir.resolve("second.txt")));
	}

	@Test
	void commitAddedFiles_healthyRepo_commitsNormally() throws Exception {
		initWithCommit();
		Files.writeString(repoDir.resolve("second.txt"), "two");
		GitRepoUtils.addFilesRelativeToRepo(repoDir.toString(), Arrays.asList("second.txt"));
		GitRepoUtils.commitAddedFiles(repoDir.toString(), "save second", "Test", "test@test.com");
		try (Git git = Git.open(repoDir.toFile())) {
			int count = 0;
			RevCommit latest = null;
			for (RevCommit commit : git.log().call()) {
				if (latest == null) {
					latest = commit;
				}
				count++;
			}
			assertEquals(2, count, "history is preserved when the repository is healthy");
			assertEquals("save second", latest.getFullMessage());
		}
	}

	@Test
	void addFilesRelativeToRepo_doesNotRewritePathsContainingVersion() throws Exception {
		initWithCommit();
		Files.createDirectories(repoDir.resolve("assets"));
		Files.writeString(repoDir.resolve("assets/conversion-notes.txt"), "v");
		GitRepoUtils.addFilesRelativeToRepo(repoDir.toString(), Arrays.asList("assets/conversion-notes.txt"));
		try (Git git = Git.open(repoDir.toFile())) {
			assertTrue(git.status().call().getAdded().contains("assets/conversion-notes.txt"));
		}
	}

	@Test
	void isCorruptRepositoryError_onlyForMissingOrCorruptObjects() {
		MissingObjectException missing = new MissingObjectException(ObjectId.zeroId(), "unknown");
		assertTrue(GitRepoUtils.isCorruptRepositoryError(new JGitInternalException(missing.getMessage(), missing)));
		assertTrue(GitRepoUtils.isCorruptRepositoryError(new RuntimeException(new IOException(missing))));
		assertFalse(GitRepoUtils.isCorruptRepositoryError(new JGitInternalException("lock failed", new IOException("x"))));
		assertFalse(GitRepoUtils.isCorruptRepositoryError(null));
	}

	@Test
	void reinitializeRepository_emptyWorkingTree_succeedsWithoutCommit() throws Exception {
		Files.createDirectories(repoDir.resolve(".git"));
		Files.writeString(repoDir.resolve(".git/HEAD"), "ref: refs/heads/master\n");
		assertTrue(GitRepoUtils.reinitializeRepository(repoDir.toString(), null, null, null));
		try (Git git = Git.open(repoDir.toFile())) {
			assertTrue(git.status().call().isClean() || !git.status().call().getUntracked().isEmpty());
		}
	}
}
