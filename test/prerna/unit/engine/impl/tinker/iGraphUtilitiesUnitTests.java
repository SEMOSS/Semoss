package prerna.unit.engine.impl.tinker;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import prerna.ds.TinkerFrame;
import prerna.engine.impl.tinker.TinkerUtilities;
import prerna.engine.impl.tinker.iGraphUtilities;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;

public class iGraphUtilitiesUnitTests {

	@Test
	public void testSynchronizeGraphToR() {
		// test setup
		TinkerFrame tf = mock(TinkerFrame.class);
		AbstractRJavaTranslator rJavaTranslator = mock(AbstractRJavaTranslator.class);
		Logger logger = mock(Logger.class);

		TinkerUtilities tinkerUtils = mock(TinkerUtilities.class);
		String dir = "my\\dir";
//		Mockito.when(tinkerUtils.serializeGraph(tf, dir)).thenReturn("my/dir/output.xml");
		String graphName = "graph";
		
		// run test code
		iGraphUtilities.synchronizeGraphToR(tf, rJavaTranslator, graphName, dir, logger);
		
		// validate
	}
	
	@Test
	public void testRemoveNodeFromR() {
		AbstractRJavaTranslator rJavaTranslator = mock(AbstractRJavaTranslator.class);
		Logger logger = mock(Logger.class);
		String graphName = "graph";
		String type = "person";
		List<Object> nodeList = new Vector<>();
		nodeList.add("tester");
		nodeList.add("user");

		// run test code
		iGraphUtilities.removeNodeFromR(graphName, rJavaTranslator, type, nodeList, logger);
		
		// validate
	}
}
