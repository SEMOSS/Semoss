package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class VectorDatabaseUtilsUnitTests {
	public static final String SOURCE = "Source";
	public static final String MODALITY = "Modality";
	public static final String DIVIDER = "Divider";
	public static final String PART = "Part";
	public static final String TOKENS = "Tokens";
	public static final String CONTENT = "Content";
	
	final private String source = "source";
	final private String modality = "modality";
	final private String divider = "divider";
	final private String part = "part";
	final private int tokens = 10;
	final private String content = "content";
	
	@Test
	void testConvertFilesToCSV(@TempDir Path tempDir) throws Exception {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "newFile1.txt";
		Path newFilePath = mainDirPath.resolve(fileName);
		Files.createFile(newFilePath);
		String titleStr = String.join(",", Arrays.asList(SOURCE, MODALITY, DIVIDER, PART, CONTENT));
		String contentStr = String.join(",", Arrays.asList(source, modality, divider, part, content));
		List<String> lines = Arrays.asList(titleStr, contentStr);
	    Files.write(newFilePath, lines);
	    File newFile = newFilePath.toFile();
	    assertLinesMatch(lines, Files.readAllLines(newFilePath));
	    
		String processedFileName = "processedFile.csv";
	    Path processedFilePath = mainDirPath.resolve(processedFileName);

	    int rowsWritten = VectorDatabaseUtils.convertFilesToCSV(processedFilePath.toString(), newFile);

	    assertTrue(Files.exists(processedFilePath));
		String updatedContentStr = String.join("\",\"",
				Arrays.asList(fileName, "text", "1", "0", titleStr + " " + contentStr));
		updatedContentStr = "\"" + updatedContentStr + "\"";
		lines = Arrays.asList(titleStr, updatedContentStr);
		assertLinesMatch(lines, Files.readAllLines(processedFilePath));
	    assertEquals(1, rowsWritten);
	}
}
