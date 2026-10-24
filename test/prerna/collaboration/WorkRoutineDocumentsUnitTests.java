package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The file side of a routine's reference documents, apart from the owner's user asset project. */
class WorkRoutineDocumentsUnitTests {

	@TempDir
	Path temp;

	@Test
	void listsVisibleFilesByNameWithSizeAndTime() throws Exception {
		Path folder = Files.createDirectories(temp.resolve("routine"));
		Files.writeString(folder.resolve("plan.md"), "plan");
		Files.writeString(folder.resolve("notes.txt"), "notes");
		Files.writeString(folder.resolve(".DS_Store"), "");
		Files.createDirectories(folder.resolve("drafts"));

		List<Map<String, Object>> listed = WorkRoutineDocuments.list(folder);

		assertEquals(List.of("notes.txt", "plan.md"), listed.stream().map(d -> d.get("name")).toList());
		assertEquals(4L, listed.get(1).get("size"));
		assertTrue(String.valueOf(listed.get(1).get("updatedAt")).endsWith("Z"));
		assertEquals(List.of(), WorkRoutineDocuments.list(temp.resolve("none")));
	}

	@Test
	void copyIntoCopiesVisibleFilesAndLeavesTheOriginals() throws Exception {
		Path folder = Files.createDirectories(temp.resolve("routine"));
		Files.writeString(folder.resolve("b.csv"), "b");
		Files.writeString(folder.resolve("a.pdf"), "a");
		Files.writeString(folder.resolve(".DS_Store"), "");
		Files.createDirectories(folder.resolve("sub"));
		Path target = temp.resolve("room").resolve(WorkRoutineDocuments.FOLDER);

		assertEquals(List.of("a.pdf", "b.csv"), WorkRoutineDocuments.copyInto(folder, target));

		assertEquals("a", Files.readString(target.resolve("a.pdf")));
		assertTrue(Files.exists(folder.resolve("a.pdf")));
		assertFalse(Files.exists(target.resolve(".DS_Store")));
		assertFalse(Files.exists(target.resolve("sub")));
	}

	@Test
	void copyIntoReplacesAnEarlierCopy() throws Exception {
		Path folder = Files.createDirectories(temp.resolve("routine"));
		Files.writeString(folder.resolve("plan.md"), "newer");
		Path target = Files.createDirectories(temp.resolve("room").resolve(WorkRoutineDocuments.FOLDER));
		Files.writeString(target.resolve("plan.md"), "old");

		WorkRoutineDocuments.copyInto(folder, target);

		assertEquals("newer", Files.readString(target.resolve("plan.md")));
	}

	@Test
	void copyIntoMakesNoFolderWhenThereIsNothingToCopy() throws Exception {
		Path target = temp.resolve("room").resolve(WorkRoutineDocuments.FOLDER);

		assertEquals(List.of(), WorkRoutineDocuments.copyInto(temp.resolve("none"), target));

		assertFalse(Files.exists(target));
	}
}
