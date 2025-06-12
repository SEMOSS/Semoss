package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class VectorDatabaseCSVWriterUnitTests {
	// used by csv file reader
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
	void testWriterConstructor(@TempDir Path tempDir) throws Exception {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "newFile1.csv";
		Path newFilePath = mainDirPath.resolve(fileName);
		VectorDatabaseCSVWriter writer =  null;
		try {
			writer = new VectorDatabaseCSVWriter(newFilePath.toString());
			assertTrue(Files.exists(newFilePath));
		} catch (Throwable t) {
			throw t;
		} finally {
			writer.close();
		}
	}
	
	@Test
	void testWriteHeader(@TempDir Path tempDir) throws Exception {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "newFile1.csv";
		Path newFilePath = mainDirPath.resolve(fileName);
		VectorDatabaseCSVWriter writer =  null;
		try {
			writer = new VectorDatabaseCSVWriter(newFilePath.toString());
			assertTrue(Files.exists(newFilePath));
			writer.writeRow(source, divider, content);
			
			String titleStr = String.join(",", Arrays.asList(SOURCE, MODALITY, DIVIDER, PART, CONTENT));
			String contentStr = String.join("\",\"", Arrays.asList(source, "text", divider, "0", content));
			contentStr = "\"" + contentStr + "\"";
			List<String> lines = Arrays.asList(titleStr, contentStr);
			
		    assertLinesMatch(lines, Files.readAllLines(newFilePath));
		    assertEquals(1, writer.getRowsInCsv());
		} catch (Throwable t) {
			throw t;
		} finally {
			writer.close();
		}
	}
}
