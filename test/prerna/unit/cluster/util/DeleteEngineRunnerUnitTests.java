package prerna.unit.cluster.util;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteEngineRunner;
import prerna.engine.api.IEngine;

public class DeleteEngineRunnerUnitTests {

	private DeleteEngineRunner reactor;
	private MockedStatic<ClusterUtil> mockedStaticClusterUtil ;
	@BeforeEach
	void setup() {
		reactor = new DeleteEngineRunner(null, null);

		mockedStaticClusterUtil = Mockito.mockStatic(ClusterUtil.class);
	}
	@AfterEach
	void tearDown() {
		// Close static mocks
		if (mockedStaticClusterUtil != null) {
			mockedStaticClusterUtil.close();
		}
	}

	@Test
	public void testRun() {

		mockedStaticClusterUtil.when(() -> ClusterUtil.deleteEngine(anyString(), any(IEngine.CATALOG_TYPE.class)))
		.thenAnswer(invocation -> null);		
		assertDoesNotThrow(() -> reactor.run());
	}
	
	// the inner method call fails but this reactor only catches that failure, doesn't propagated the failure up
	@Test
	public void testFail() {
		mockedStaticClusterUtil.when(() -> ClusterUtil.deleteEngine(anyString(), any(IEngine.CATALOG_TYPE.class))).thenThrow(new RuntimeException());
		assertDoesNotThrow(() -> reactor.run());
	}

}

