package prerna.testing.reactor.imports;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.testing.AbstractBaseSemossApiTests;

public class ImportReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testImportToGrid() {
		String frameType = "Grid";
		String frameAlias = "Frame123";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createFrame(frameType, frameAlias, override);
		assertEquals(DataFrameTypeEnum.GRID, frame.getFrameType());
		assertEquals(frameAlias, frame.getName());
	}

	@Test
	public void testImportToPythonFrame() {
		String frameType = "Py";
		String frameAlias = "Frame123";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createFrame(frameType, frameAlias, override);
		assertEquals(DataFrameTypeEnum.PYTHON, frame.getFrameType());
		assertEquals(frameAlias, frame.getName());
	}
	
}
