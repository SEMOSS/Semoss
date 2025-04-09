package prerna.unit.cluster.util;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.AdminPushLocalToCloudReactor;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

public class ClusterUtilUnitTests {

	private ClusterUtil clusterUtilClass;
	@BeforeEach
	private void setUp() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		// need to set final field value so using reflection
		Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER");
		field.setAccessible(true);

		// remove final modifier
		Field modifiersField = Field.class.getDeclaredField("modifiers");
		modifiersField.setAccessible(true);
		modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);

		// set new value
		field.set(ClusterUtil.IS_CLUSTER, true);


		clusterUtilClass = new ClusterUtil();
	}

	@AfterEach
	private void tearDown() {

	}

	@Test
	public void testIsSchedulerExecutorgetDIHelperPropertyReturn() {
		try(MockedStatic<Utility> staticUtility = Mockito.mockStatic(Utility.class)){
			staticUtility.when(()-> Utility.getDIHelperProperty(anyString())).thenReturn("true");

			assertTrue(ClusterUtil.isSchedulerExecutor());
		}
	}

	/**
	 * use junit pioneer to set environment value, should return true
	 */
	@Test
	@SetEnvironmentVariable(key = "SCHEDULER_EXECUTOR", value = "true")
	public void testIsSchedulerExecutorLeaderReturn()  {
		assertEquals(true, ClusterUtil.isSchedulerExecutor());
	}

	@Test
	public void testIsSchedulerExecutorIsClusterFalse() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER");
		field.setAccessible(true);

		// remove final modifier
		Field modifiersField = Field.class.getDeclaredField("modifiers");
		modifiersField.setAccessible(true);
		modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);

		// set new value
		field.set(ClusterUtil.IS_CLUSTER, false);

		try(MockedStatic<Utility> staticUtility = Mockito.mockStatic(Utility.class)){
			staticUtility.when(()-> Utility.getDIHelperProperty(anyString())).thenReturn("true");

			assertTrue(ClusterUtil.isSchedulerExecutor());
		}
	}

	@Test
	public void testPullEngineSuccessOneArg() throws IOException, InterruptedException {
		CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class)){
			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).pullEngine(anyString());

			assertDoesNotThrow(()->ClusterUtil.pullEngine("engineId"));
		}
	}

	@Test
	public void testPullEngineFailOneArg() throws IOException, InterruptedException {
		SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullEngine("engineId"));
		assertEquals("Failed to pull engine 'engineId' from cloud storage" ,e.getMessage());
	}

	@Test
	public void testPullEngineSuccessTwoArg() throws IOException, InterruptedException {
		CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class)){
			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
						doNothing().when(mockedCCS).pullEngine(anyString());

			assertDoesNotThrow(()->ClusterUtil.pullEngine("engineId", IEngine.CATALOG_TYPE.DATABASE));
		}
	}

	@Test
	public void testPullEngineFailTwoArg() throws IOException, InterruptedException {
		SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullEngine("engineId", IEngine.CATALOG_TYPE.DATABASE));
		assertEquals("Failed to pull engine 'engineId' from cloud storage" ,e.getMessage());	
	}

	@Test
	public void testPullEngineSuccessThreeArg() throws IOException, InterruptedException {
		CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class)){
			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).pullEngine(anyString());

			assertDoesNotThrow(()->ClusterUtil.pullEngine("engineId", IEngine.CATALOG_TYPE.DATABASE, true));
		}
	}

	@Test
	public void testPullEngineFailThreeArg() throws IOException, InterruptedException {
		SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullEngine("engineId", IEngine.CATALOG_TYPE.DATABASE, true));
		assertEquals("Failed to pull engine 'engineId' from cloud storage" ,e.getMessage());	
	}

}

