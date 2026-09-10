package prerna.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileSystemUtilCopyResolvedFileTest {

	@TempDir
	Path source;

	@TempDir
	Path target;

	@Test
	void copiesIntoMissingParentFolders() throws Exception {
		Path file = source.resolve("deck.pptx");
		Files.write(file, new byte[] { 1, 2, 3 });
		Path destination = target.resolve("version/assets/office-studio/presentations/deck-v1.pptx");
		long size = FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), false);
		assertEquals(3, size);
		assertTrue(Files.exists(destination));
		assertTrue(Files.exists(file), "source is left in place");
	}

	@Test
	void refusesExistingDestinationUnlessOverride() throws Exception {
		Path file = source.resolve("deck.pptx");
		Files.write(file, new byte[] { 9, 9 });
		Path destination = target.resolve("deck.pptx");
		Files.write(destination, new byte[] { 1 });
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), false));
		assertTrue(error.getMessage().contains("override=true"));
		assertFalse(error.getMessage().contains(target.toString()), "error must not leak absolute paths");
		assertEquals(1, Files.size(destination));

		assertEquals(2, FileSystemUtil.copyResolvedFile(file.toString(), destination.toString(), true));
		assertEquals(2, Files.size(destination));
	}

	@Test
	void rejectsDirectoriesAndSelfCopies() throws Exception {
		Path folder = source.resolve("folder");
		Files.createDirectories(folder);
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(folder.toString(), target.resolve("x").toString(), true));
		Path file = source.resolve("a.txt");
		Files.writeString(file, "a");
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(file.toString(), file.toString(), true));
		assertThrows(IllegalArgumentException.class,
				() -> FileSystemUtil.copyResolvedFile(source.resolve("missing").toString(), target.resolve("x").toString(), true));
	}
}
