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

import prerna.cache.ICache;

public class ICacheUnitTests {
	

	@ParameterizedTest
	@ValueSource(strings = { " ", "\t", "\n" })
	public void cleanFolderAndFileName_weirdChars(String test) {
		assertEquals("_", ICache.cleanFolderAndFileName(test));
	}
	
	@Test
	public void cleanFolderAndFileName() {
		assertEquals(null, ICache.cleanFolderAndFileName(null));
		assertEquals("", ICache.cleanFolderAndFileName(""));
		assertEquals("Hello_world.txt", ICache.cleanFolderAndFileName("Hello_world.txt"));
		assertEquals("Hello_world_1_.txt", ICache.cleanFolderAndFileName("Hello_world(1).txt"));
	}
    
    @Test
   	public void testWriteToFile(@TempDir File tempDir) {
        File testFile = new File(tempDir, "test.txt");
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
   	public void testReadFromFileString(@TempDir File tempDir) {
    	// test reading empty file
    	File testFile = new File(tempDir, "test.txt");
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
    
    /**
     * Read from file that is a dir
     * @param tempDir
     */
    @Test
   	public void testReadFromFileStringDir(@TempDir File tempDir) {
    	// test reading dir file
    	File testFile = new File(tempDir, "test");
    	testFile.mkdir();
   		String filePath = testFile.getAbsolutePath();
   		String fileContents = ICache.readFromFileString(filePath);
   		assertEquals(null, fileContents);
   	}
    
    @Test
   	public void testDeleteFolder(@TempDir File tempDir) {
    	// create test dir
      	File testFile = new File(tempDir, "test");
      	testFile.mkdir();
      	assertTrue(testFile.exists());
      	String filePath = testFile.getAbsolutePath();
      	
      	// run delete
   		ICache.deleteFolder(filePath);
   		
   		// validate delete
      	assertFalse(testFile.exists());
      	
   	}
    
    @Test
   	public void testDeleteFolderUsingFile(@TempDir File tempDir) throws IOException {
    	// create test dir
      	File testFile = new File(tempDir, "test.txt");
      	testFile.createNewFile();
      	assertTrue(testFile.exists());
      	String filePath = testFile.getAbsolutePath();
      	
      	// run delete
   		ICache.deleteFolder(filePath);
   		
   		// validate delete did not work
      	assertTrue(testFile.exists());
      	
   	}
    
    @Test
   	public void testDeleteFolderWithFile(@TempDir File tempDir) throws IOException {
    	// create test dir
      	File testFile = new File(tempDir, "testFolder");
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
   	public void testDeleteFolderFromFile(@TempDir File tempDir) {
    	// create test dir
      	File testFile = new File(tempDir, "test");
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
	public void deleteFile(@TempDir File tempDir) throws IOException {
		// add file
		File file = new File(tempDir, "hello.txt");
		file.createNewFile();
		assertTrue(file.exists());
		ICache.deleteFile(file);
		assertFalse(file.exists());
   	}
    
    @Test
   	public void deleteFileStr(@TempDir File tempDir) throws IOException {
		File file = new File(tempDir, "hello.txt");
		file.createNewFile();
		assertTrue(file.exists());
   		String filePath = file.getAbsolutePath();
		ICache.deleteFile(filePath);
		assertFalse(file.exists());
   	}
    
    @Test
   	public void deleteFileStrUsingDir(@TempDir File tempDir) throws IOException {
		File file = new File(tempDir, "hello.txt");
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
