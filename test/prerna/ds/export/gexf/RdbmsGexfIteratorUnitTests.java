package prerna.ds.export.gexf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class RdbmsGexfIteratorUnitTests {

	private String nodeMap = "Movie,MovieBudget,Nominated;Studio;Director";
	private String edgeMap = "Movie,Studio;Movie,Director,Rating;";
	String[] colNames = new String[] { "Movie", "Studio", "Director", "MovieBudget", "Rating", "Nominated" };
	String[] types = new String[] { "STRING", "STRING", "STRING", "INT", "DOUBLE", "STRING" };

	Object[] values = new Object[] { "Shark Tale", "Dreamworks", "Vicky Jenson", 1000, 8.8, "YES" };
	private Map<String, String> aliasMap = new HashMap<>();

	@BeforeEach
	void setup() {
		// node alias
		aliasMap.put("Movie", "Title");

		// node property alias
		aliasMap.put("MovieBudget", "Budget");

		// edge property alias
		aliasMap.put("Rating", "Director_Rating");
	}

	@Test
	void testHasNextNode(@TempDir File tempDir) {
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File insightCacheFolder = new File(baseFolderPath, "insight");
		insightCacheFolder.mkdir();

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			when(instance.getProperty(Constants.INSIGHT_CACHE_DIR)).thenReturn(insightCacheFolder.getAbsolutePath());
			H2Frame frame = new H2Frame(colNames, types);
			frame.addRow(values, colNames);
			RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, aliasMap);
			assertTrue(it.hasNextNode());
			assertTrue(it.hasNextNode());
			assertTrue(it.hasNextNode());
			assertFalse(it.hasNextNode());
			frame.close();
		}
	}

	@Test
	void testGetNextNode(@TempDir File tempDir) {
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File insightCacheFolder = new File(baseFolderPath, "insight");
		insightCacheFolder.mkdir();

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			when(instance.getProperty(Constants.INSIGHT_CACHE_DIR)).thenReturn(insightCacheFolder.getAbsolutePath());
			H2Frame frame = new H2Frame(colNames, types);
			frame.addRow(values, colNames);

			// test no alias map
			{
				RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, null);
				// check first node
				assertTrue(it.hasNextNode());
				String node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Shark Tale\" value=\"Shark Tale\"><attvalues><attvalue for=\"type\" value=\"Movie\"/><attvalue for=\"MovieBudget\" value=1000/><attvalue for=\"Nominated\" value=\"YES\"/></attvalues></node>",
						node);
				// check second node
				assertTrue(it.hasNextNode());
				node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Dreamworks\" value=\"Dreamworks\"><attvalues><attvalue for=\"type\" value=\"Studio\"/></attvalues></node>",
						node);
				// check second node
				assertTrue(it.hasNextNode());
				node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Vicky Jenson\" value=\"Vicky Jenson\"><attvalues><attvalue for=\"type\" value=\"Director\"/></attvalues></node>",
						node);
				// end of nodes
				assertFalse(it.hasNextNode());
			}
			
			// test with alias map
			{
				RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, aliasMap);
				// check first node
				assertTrue(it.hasNextNode());
				String node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Shark Tale\" value=\"Shark Tale\"><attvalues><attvalue for=\"type\" value=\"Title\"/><attvalue for=\"Budget\" value=1000/><attvalue for=\"Nominated\" value=\"YES\"/></attvalues></node>",
						node);
				// check second node
				assertTrue(it.hasNextNode());
				node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Dreamworks\" value=\"Dreamworks\"><attvalues><attvalue for=\"type\" value=\"Studio\"/></attvalues></node>",
						node);
				// check second node
				assertTrue(it.hasNextNode());
				node = it.getNextNodeString();
				assertEquals(
						"<node id=\"Vicky Jenson\" value=\"Vicky Jenson\"><attvalues><attvalue for=\"type\" value=\"Director\"/></attvalues></node>",
						node);
				// end of nodes
				assertFalse(it.hasNextNode());
			}
			frame.close();
		}
	}

	@Test
	void testHasNextEdge(@TempDir File tempDir) {
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File insightCacheFolder = new File(baseFolderPath, "insight");
		insightCacheFolder.mkdir();

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			when(instance.getProperty(Constants.INSIGHT_CACHE_DIR)).thenReturn(insightCacheFolder.getAbsolutePath());
			H2Frame frame = new H2Frame(colNames, types);
			frame.addRow(values, colNames);

			RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, aliasMap);
			assertTrue(it.hasNextEdge());
			assertTrue(it.hasNextEdge());
			assertFalse(it.hasNextEdge());
			frame.close();
		}
	}

	@Test
	void testGetNextEdge(@TempDir File tempDir) {
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File insightCacheFolder = new File(baseFolderPath, "insight");
		insightCacheFolder.mkdir();

		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			when(instance.getProperty(Constants.INSIGHT_CACHE_DIR)).thenReturn(insightCacheFolder.getAbsolutePath());

			H2Frame frame = new H2Frame(colNames, types);
			frame.addRow(values, colNames);

			// test with no alias map
			{

				RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, null);
				assertTrue(it.hasNextEdge());

				// check first edge
				String edge = it.getNextEdgeString();
				assertEquals("<edge id=\"Shark Tale+++Dreamworks\" source=\"Shark Tale\" target=\"Dreamworks\"></edge>",
						edge);

				// check second edge
				assertTrue(it.hasNextEdge());
				edge = it.getNextEdgeString();
				assertEquals(
						"<edge id=\"Shark Tale+++Vicky Jenson\" source=\"Shark Tale\" target=\"Vicky Jenson\"><attvalues><attvalue for=\"Rating\" value=8.8/></attvalues></edge>",
						edge);

				// make sure no more edges
				assertFalse(it.hasNextEdge());
			}

			// test with alias map
			{

				RdbmsGexfIterator it = new RdbmsGexfIterator(frame, nodeMap, edgeMap, aliasMap);
				assertTrue(it.hasNextEdge());

				// check first edge
				String edge = it.getNextEdgeString();
				assertEquals("<edge id=\"Shark Tale+++Dreamworks\" source=\"Shark Tale\" target=\"Dreamworks\"></edge>",
						edge);

				// check second edge
				assertTrue(it.hasNextEdge());
				edge = it.getNextEdgeString();
				assertEquals(
						"<edge id=\"Shark Tale+++Vicky Jenson\" source=\"Shark Tale\" target=\"Vicky Jenson\"><attvalues><attvalue for=\"Director_Rating\" value=8.8/></attvalues></edge>",
						edge);

				// make sure no more edges
				assertFalse(it.hasNextEdge());
			}
			frame.close();
		}
	}
}
