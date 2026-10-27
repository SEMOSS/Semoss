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
	void copyIntoGivesAnEmailAsItsText() throws Exception {
		Path folder = Files.createDirectories(temp.resolve("routine"));
		Files.writeString(folder.resolve("pushback.eml"), "From: \"Bell, Jen\" <jbell@example.com>\r\n"
				+ "To: \"Myers, Deanette\" <dmyers@example.com>\r\nSubject: RE: Meeting Approval (102241)\r\n"
				+ "Content-Type: text/plain; charset=\"Windows-1252\"\r\nContent-Transfer-Encoding: quoted-printable\r\n"
				+ "\r\nWhat differentiates it from the official onboard=\r\ning program?\r\n");
		Files.writeString(folder.resolve("broken.msg"), "not an Outlook file");
		Path target = temp.resolve("room").resolve(WorkRoutineDocuments.FOLDER);

		assertEquals(List.of("broken.msg.txt", "pushback.eml.txt"), WorkRoutineDocuments.copyInto(folder, target));

		assertEquals("From: \"Bell, Jen\" <jbell@example.com>\nTo: \"Myers, Deanette\" <dmyers@example.com>\n"
				+ "Subject: RE: Meeting Approval (102241)\n\nWhat differentiates it from the official onboarding program?",
				Files.readString(target.resolve("pushback.eml.txt")));
		// one that cannot be read as an email still reaches the run, as it is
		assertEquals("not an Outlook file", Files.readString(target.resolve("broken.msg.txt")));
		assertFalse(Files.exists(target.resolve("pushback.eml")));
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
