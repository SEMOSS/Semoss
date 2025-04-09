package prerna.unit.cluster.util;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.lang.reflect.Field;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
		reactor = new CopyFilesToEngineRunner(engineId, IEngine.CATALOG_TYPE.DATABASE, new String[5]);
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
	public void runTest() {
		String[] array = new String[] {"a", "a", "a", "a", "a"};
		mockedStaticCU.when(()-> ClusterUtil.copyLocalFileToEngineCloudFolder(engineId, CATALOG_TYPE.DATABASE, array[0])).thenAnswer(invocation -> null);
		
		assertDoesNotThrow(reactor::run);
	}
	
	@Test
	public void failTest() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		String[] array = new String[] {"a", "a", "a", "a", "a"};
		mockedStaticCU.when(()-> ClusterUtil.copyLocalFileToEngineCloudFolder(engineId, CATALOG_TYPE.DATABASE, array[0])).thenThrow(new SemossPixelException("error"));
		
		
        // need to change final field value so need to use reflection
        Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
        field.setAccessible(true);

        // remove final modifier
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);

        // set new value
        field.set(ClusterUtil.IS_CLUSTER_ZK, true);
        
		
		assertThrows(SemossPixelException.class, reactor::run);
		
		// reset the final field value for other tests
		field.set(ClusterUtil.IS_CLUSTER_ZK, false);
	}
}
