package prerna.unit.cluster.util;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.cluster.util.ClusterSynchronizer;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.DeleteFilesFromEngineRunner;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class DeleteFilesFromEngineRunnerUnitTests {


	private DeleteFilesFromEngineRunner reactor;
	private MockedStatic<ClusterUtil> mockedStaticCU ;
	private final String engineId = "engineId";

	@BeforeEach
	void setup() {
		reactor = new DeleteFilesFromEngineRunner(engineId, IEngine.CATALOG_TYPE.DATABASE, new String[5]);
		mockedStaticCU = Mockito.mockStatic(ClusterUtil.class);
	}
	@AfterEach
	void tearDown() {
		// Close static mocks
		if (mockedStaticCU != null) {
			mockedStaticCU.close();
		}
	}

	@Test
	public void runTest() throws Exception {
		String[] array = new String[] {"a", "a", "a", "a", "a"};
		mockedStaticCU.when(()-> ClusterUtil.deleteEngineCloudFile(engineId, CATALOG_TYPE.DATABASE, array[0])).thenAnswer(invocation -> null);

	    ClusterSynchronizer mockSync = mock(ClusterSynchronizer.class);
	    mockedStaticCU.when(ClusterUtil::getClusterSynchronizer).thenReturn(mockSync);

	    doNothing().when(mockSync).publishEngineChange(anyString(), anyString(), anyString());

	    assertDoesNotThrow(reactor::run);
	}

	@Test
	public void failTest() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		String[] array = new String[] {"a", "a", "a", "a", "a"};
		mockedStaticCU.when(()-> ClusterUtil.deleteEngineCloudFile(engineId, CATALOG_TYPE.DATABASE, array[0])).thenThrow(new SemossPixelException("error"));

		assertThrows(SemossPixelException.class, reactor::run);
	}
}
