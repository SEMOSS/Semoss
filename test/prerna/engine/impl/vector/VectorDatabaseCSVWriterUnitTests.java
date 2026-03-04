/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
import prerna.SemossUnitTest;

public class VectorDatabaseCSVWriterUnitTests extends SemossUnitTest {
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
	void testWriterConstructor() throws Exception {
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
	void testWriteHeader() throws Exception {
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
