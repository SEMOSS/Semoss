/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.testing.reactor.imports;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.testing.AbstractBaseSemossApiTests;

public class ImportReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testFileRead() {
		String frameType = "Grid";
		String frameAlias = "Frame123";
		ITableDataFrame frame = ImportTestUtility.fileReadMovie(frameType, frameAlias);
		assertEquals(DataFrameTypeEnum.GRID, frame.getFrameType());
		assertEquals(frameAlias, frame.getName());
	}

	@Test
	public void testImportToGrid() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "Frame123";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
		assertEquals(DataFrameTypeEnum.GRID, frame.getFrameType());
		assertEquals(frameAlias, frame.getName());
	}

	@Test
	public void testImportToPythonFrame() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Py";
		String frameAlias = "Frame123";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
		assertEquals(DataFrameTypeEnum.PYTHON, frame.getFrameType());
		assertEquals(frameAlias, frame.getName());
	}
}
