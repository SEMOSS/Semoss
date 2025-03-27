package prerna.unit.cache;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
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
   	public void testWriteToFile() {
   		String filePath = null;
		Object vec = "test";
		ICache.writeToFile(filePath, vec );
   	}
//    
//    @Test
//   	public void testReadFromFileString() {
//   		String filePath = null;
//		ICache.readFromFileString(filePath);
//   	}
//    
//    @Test
//   	public void testDeleteFolder() {
//   		String filePath = null;
//		ICache.deleteFolder(filePath);
//   	}
//    
//    @Test
//   	public void deleteFile() {
//   		File filePath = null;
//		ICache.deleteFile(filePath);
//   	}
//    
//    @Test
//   	public void deleteFileStr() {
//   		String filePath = null;
//		ICache.deleteFile(filePath);
//   	}

}
