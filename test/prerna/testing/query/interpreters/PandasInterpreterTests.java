package prerna.testing.query.interpreters;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.PixelChain;
import prerna.testing.PixelQueryTestUtils;
import prerna.testing.reactor.imports.ImportTestUtility;

public class PandasInterpreterTests extends AbstractBaseSemossApiTests {

	@Test
	public void testAlias() {
		// import movie data to frame
		String frameType = "Py";
		String frameAlias = "pyFrame";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createMovieFrame(frameType, frameAlias, override);
		// create pixel query to test interpreter
		PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
		String[] cols = new String[] {ApiTestsSemossConstants.GENRE, ApiTestsSemossConstants.STUDIO};
		String[] alias = new String[] {"x", "y"};
		PixelChain select = PixelQueryTestUtils.select(cols, alias);
		PixelChain collectAll = PixelQueryTestUtils.collectAll();
		
		String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, collectAll);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
		List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 105);
		for (Map<String, Object> info:list) {
			assertTrue(info.containsKey("x"));
			assertTrue(info.containsKey("y"));
		}
	}
	
	@Test
	public void testGroupBy() {
		// import movie data to frame
		String frameType = "Py";
		String frameAlias = "pyFrame";
		boolean override = true;
		ITableDataFrame frame = ImportTestUtility.createMovieFrame(frameType, frameAlias, override);
		// create pixel query to test interpreter
		PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
		String[] cols = new String[] {ApiTestsSemossConstants.GENRE, "Count("+ApiTestsSemossConstants.TITLE+")"};
		String[] alias = new String[] {"gen","count_gen"};

		PixelChain select = PixelQueryTestUtils.select(cols, alias);
		PixelChain groupBy = PixelQueryTestUtils.groupBy(new String[] {ApiTestsSemossConstants.GENRE});
		PixelChain collectAll = PixelQueryTestUtils.collectAll();
		
		String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, groupBy, collectAll);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
		List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 6);
		for (Map<String, Object> info:list) {
			// TODO assertions
			ApiSemossTestUtils.print(info);
		}
	}
}
