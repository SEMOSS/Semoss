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
