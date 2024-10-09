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
