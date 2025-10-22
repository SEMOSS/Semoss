package prerna.unit.cache;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import prerna.SemossUnitTest;
import prerna.cache.ICache;

public class ICacheUnitTests extends SemossUnitTest {
	

	@ParameterizedTest
	@ValueSource(strings = { " ", "\t", "\n" })
	public void testCleanFolderAndFileName_weirdChars(String test) {
		assertEquals("_", ICache.cleanFolderAndFileName(test));
	}
	
	@Test
	public void testCleanFolderAndFileName() {
		assertEquals("", ICache.cleanFolderAndFileName(""));
		assertEquals("Hello_world.txt", ICache.cleanFolderAndFileName("Hello_world.txt"));
		assertEquals("Hello_world_1_.txt", ICache.cleanFolderAndFileName("Hello_world(1).txt"));
	}
    
    @Test
   	public void testWriteToFile() {
        File testFile = new File(tempDir.toFile(), "test.txt");
   		String filePath = testFile.getAbsolutePath();
		Object vec = "test";
		ICache.writeToFile(filePath, vec );
		String contents = null;
		try {
			contents = FileUtils.readFileToString(testFile);
			assertTrue(contents.contains(vec.toString()));
		} catch (IOException e) {
			e.printStackTrace();
			fail("unable to read file");
		}
   	}
    
    @Test
   	public void testReadFromFileString() {
    	// test reading empty file
    	File testFile = new File(tempDir.toFile(), "test.txt");
   		String filePath = testFile.getAbsolutePath();
   		String fileContents = ICache.readFromFileString(filePath);
   		assertEquals(null, fileContents);
   		
   		// add data to file
   		Object vec = "hello";
		ICache.writeToFile(filePath, vec );
		
		// test reading file data
		fileContents = ICache.readFromFileString(filePath);
   		assertEquals("\"hello\"", fileContents);
   	}

    @Test
   	public void testReadFromFileStringDir() {
    	// test reading dir file
    	File testFile = new File(tempDir.toFile(), "test");
    	testFile.mkdir();
   		String filePath = testFile.getAbsolutePath();
   		String fileContents = ICache.readFromFileString(filePath);
   		assertEquals(null, fileContents);
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
    
    @Test
   	public void testDeleteFileStr() throws IOException {
		File file = new File(tempDir.toFile(), "hello.txt");
		file.createNewFile();
		assertTrue(file.exists());
   		String filePath = file.getAbsolutePath();
		ICache.deleteFile(filePath);
		assertFalse(file.exists());
   	}
    
    @Test
   	public void testDeleteFileStrUsingDir() throws IOException {
		File file = new File(tempDir.toFile(), "hello.txt");
		assertFalse(file.exists());
   		String filePath = file.getAbsolutePath();
		ICache.deleteFile(filePath);
		
		file.mkdir();
		assertTrue(file.exists());
		filePath = file.getAbsolutePath();
		ICache.deleteFile(filePath);
		// assert unable to delete
		assertTrue(file.exists());

   	}

}
