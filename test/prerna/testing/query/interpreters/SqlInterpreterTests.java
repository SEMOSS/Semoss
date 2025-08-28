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
package prerna.testing.query.interpreters;

import org.junit.jupiter.api.Test;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.reactor.imports.ImportTestUtility;

public class SqlInterpreterTests extends AbstractBaseSemossApiTests {

	@Test
	public void testSort() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "grid123";
		boolean override = true;
		InterpreterTests.testSort(databaseId, frameType, frameAlias, override);
	}

	@Test
	public void testAlias() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "grid123";
		boolean override = true;
		InterpreterTests.testLimitOffset(databaseId, frameType, frameAlias, override);
		InterpreterTests.testAlias(databaseId, frameType, frameAlias, override);
		InterpreterTests.testDistinct(databaseId, frameType, frameAlias, override);
	}

	@Test
	public void testGroupBy() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "grid123";
		boolean override = true;
		InterpreterTests.testGroupBy(databaseId, frameType, frameAlias, override);
	}

	@Test
	public void testFilter() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "grid123";
		boolean override = true;
		InterpreterTests.testFilter(databaseId, frameType, frameAlias, override);
	}

	@Test
	public void testGroupFilter() {
		// import movie data to frame
		String databaseName = "MOV_DB";
		String databaseId = ImportTestUtility.uploadMovieDB(databaseName);

		String frameType = "Grid";
		String frameAlias = "grid123";
		boolean override = true;
		InterpreterTests.testGroupFilter(databaseId, frameType, frameAlias, override);
	}
}
