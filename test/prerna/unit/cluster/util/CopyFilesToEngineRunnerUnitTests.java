package prerna.unit.cluster.util;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
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
import prerna.cluster.util.CopyFilesToEngineRunner;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.sablecc2.om.execptions.SemossPixelException;


public class CopyFilesToEngineRunnerUnitTests {

	private CopyFilesToEngineRunner reactor;
	private MockedStatic<ClusterUtil> mockedStaticCU ;
	private final String engineId = "engineId";
	@BeforeEach
	void setup() {
		String[] testArray = new String[5];
		testArray[0] = "alpha";
		testArray[1] = "beta";
		testArray[2] = "gamma";
		testArray[3] = "delta";
		testArray[4] = "epsilon";
		
		reactor = new CopyFilesToEngineRunner(engineId, IEngine.CATALOG_TYPE.DATABASE, testArray);
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
		mockedStaticCU.when(()-> ClusterUtil.copyLocalFileToEngineCloudFolder(anyString(), any(CATALOG_TYPE.class), anyString())).thenAnswer(invocation -> null);
		
	    ClusterSynchronizer mockSync = mock(ClusterSynchronizer.class);
	    mockedStaticCU.when(ClusterUtil::getClusterSynchronizer).thenReturn(mockSync);

	    doNothing().when(mockSync).publishEngineChange(anyString(), anyString(), anyString());

	    assertDoesNotThrow(reactor::run);
	}
	
	@Test
	public void failTest() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		mockedStaticCU.when(()-> ClusterUtil.copyLocalFileToEngineCloudFolder(anyString(), any(CATALOG_TYPE.class), anyString())).thenThrow(new SemossPixelException("error"));
		
		assertThrows(SemossPixelException.class, reactor::run);
	}
}
