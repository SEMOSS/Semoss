package prerna.unit.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;
import prerna.cache.ICache;

public class ICacheUnitTests extends SemossUnitTest {

	@BeforeEach
	void setUp() throws IOException {
		FileUtils.cleanDirectory(tempDir.toFile());
	}

	@Test
	public void testDeleteFolder() {
		// create test dir
		File testFile = new File(tempDir.toFile(), "test");
		testFile.mkdir();
		assertTrue(testFile.exists());
		String filePath = testFile.getAbsolutePath();

		// run delete
		ICache.deleteFolder(filePath);

		// validate delete
		assertFalse(testFile.exists());
	}

	@Test
	public void testDeleteFolderUsingFile() throws IOException {
		// create test dir
		File testFile = new File(tempDir.toFile(), "test.txt");
		testFile.createNewFile();
		assertTrue(testFile.exists());
		String filePath = testFile.getAbsolutePath();

		// run delete
		ICache.deleteFolder(filePath);

		// validate delete did not work
		assertTrue(testFile.exists());
	}

	@Test
	public void testDeleteFolderWithFile() throws IOException {
		// create test dir
		File testFile = new File(tempDir.toFile(), "testFolder");
		testFile.mkdir();
		assertTrue(testFile.exists());
		String filePath = testFile.getAbsolutePath();

		// add file
		File file = new File(testFile, "hello.txt");
		file.createNewFile();
		assertTrue(file.exists());

		// run delete
		ICache.deleteFolder(filePath);

		// validate delete
		assertFalse(testFile.exists());
	}

	@Test
	public void testDeleteFolderFromFile() {
		// create test dir
		File testFile = new File(tempDir.toFile(), "test");
		testFile.mkdir();
		assertTrue(testFile.exists());

		// run delete
		ICache.deleteFolder(testFile);

		// validate delete
		assertFalse(testFile.exists());

		// run delete again
		ICache.deleteFolder(testFile);
	}

	@Test
	public void testDeleteFile() throws IOException {
		// add file
		File file = new File(tempDir.toFile(), "hello.txt");
		file.createNewFile();
		assertTrue(file.exists());
		ICache.deleteFile(file);
		assertFalse(file.exists());
	}

}
