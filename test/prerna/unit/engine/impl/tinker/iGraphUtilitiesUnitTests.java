package prerna.unit.engine.impl.tinker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerUtilities;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;

public class iGraphUtilitiesUnitTests {

	@Test
	public void testSynchronizeGraphToR() {
		// test setup
		TinkerFrame tf = new TinkerFrame();
		AbstractRJavaTranslator rJavaTranslator = mock(AbstractRJavaTranslator.class);
		Logger logger = mock(Logger.class);
		String graphName = "graph";
		String dir = "my\\dir";

		// mock util used to serialize graph
		try (MockedStatic<TinkerUtilities> tinkerUtils = Mockito.mockStatic(TinkerUtilities.class)) {
			tinkerUtils.when(() -> TinkerUtilities.serializeGraph(tf, dir)).thenReturn("my/dir/output.xml");

			// run test code
			iGraphUtilities.synchronizeGraphToR(tf, rJavaTranslator, graphName, dir, logger);
		}
	
		// validate
		ArgumentCaptor<String> strCaptor = ArgumentCaptor.forClass(String.class);
		verify(rJavaTranslator, times(1)).executeEmptyR(strCaptor.capture());
		String rSyntax = strCaptor.getValue();
		String expectedRSyntax = "library(\"igraph\");graph<- read_graph(\"my/dir/output.xml\", \"graphml\");";
		assertEquals(expectedRSyntax, rSyntax);
		assertTrue(tf.isIGraphSynched());
	}
	
	@Test
	public void testRemoveNodeFromR() {
		AbstractRJavaTranslator rJavaTranslator = mock(AbstractRJavaTranslator.class);
		Logger logger = mock(Logger.class);
		String graphName = "graph";
		String type = "person";
		List<Object> nodeList = new Vector<>();
		nodeList.add("test user");

		// run test code
		iGraphUtilities.removeNodeFromR(graphName, rJavaTranslator, type, nodeList, logger);
		
		// validate
		ArgumentCaptor<String> strCaptor = ArgumentCaptor.forClass(String.class);
		verify(rJavaTranslator, times(1)).executeEmptyR(strCaptor.capture());
		String rSyntax = strCaptor.getValue();
		String expectedRSyntax = "graph <- delete_vertices(graph, V(graph)[vertex_attr(graph, \"_T_ID\") == \"person:test user\"])";
		assertEquals(expectedRSyntax, rSyntax);
	}
}
