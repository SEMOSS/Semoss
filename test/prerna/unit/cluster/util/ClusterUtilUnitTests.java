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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.cluster.util.AdminPushLocalToCloudReactor;
import prerna.cluster.util.ClusterSynchronizer;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Utility;

public class ClusterUtilUnitTests {

	private ClusterUtil clusterUtilClass;
	private MockedStatic<CentralCloudStorage> mockedStaticCCS ;

	@BeforeEach
	private void setUp() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
		clusterUtilClass = new ClusterUtil();
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
	public void testIsSchedulerExecutorLeaderReturn()  {
		assertEquals(true, ClusterUtil.isSchedulerExecutor());
	}

	@Test
	public void testIsSchedulerExecutorIsClusterFalse() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
//		Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER");
//		field.setAccessible(true);

//		// remove final modifier
//		Field modifiersField = Field.class.getDeclaredField("modifiers");
//		modifiersField.setAccessible(true);
//		modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);

//		// set new value
//		field.set(ClusterUtil.IS_CLUSTER, false);

		try(MockedStatic<Utility> staticUtility = Mockito.mockStatic(Utility.class)){
			staticUtility.when(()-> Utility.getDIHelperProperty(anyString())).thenReturn("true");

			assertTrue(ClusterUtil.isSchedulerExecutor());
		}
	}

	/**
	 * multiple versions of the pullEngine() method exists, 1-3 args, testing all
	 */
	@Nested
	class testpullEngine {
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


	@Test
	public void testPushEngineSuccess() throws Exception {
		//first condition in method
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
				MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);
				){
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).pushEngine(anyString());

			//----------------------------------------------------------------------------
			// second condition in method

			ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

			mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
			doNothing().when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

			assertDoesNotThrow(()->ClusterUtil.pushEngine("engineId"));
		}
	}
	@Test
	public void testPushEngineFail() throws Exception {
		//first condition in method
		CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
				MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);){

			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doThrow(IOException.class).when(mockedCCS).pushEngine(anyString());

			SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngine("engineId"));
			assertEquals("Failed to push engine 'engineId' to cloud storage" ,e.getMessage());

			//----------------------------------------------------------------------------

			// second condition in method
			Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
			field.setAccessible(true);

//			// remove final modifier
//			Field modifiersField = Field.class.getDeclaredField("modifiers");
//			modifiersField.setAccessible(true);
//
//			modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
//			// set new value
//			field.set(ClusterUtil.IS_CLUSTER_ZK, true);

			doNothing().when(mockedCCS).pushEngine(anyString());

			ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

			mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
			doThrow(IOException.class).when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngine("engineId"));
			assertEquals("Failed to publish engine 'engineId' to sync with ZK cluster" ,e2.getMessage());
		}
	}

	/**
	 * multiple versions of the pushEngineSmss() method exists, 1-2 args, testing all
	 */
	@Nested
	class testPushEngineSmss {
		@Test
		public void testPushEngineSmssSuccess() throws Exception {
			//first condition in method
			try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);
					){
				CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
				mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
				doNothing().when(mockedCCS).pushEngineSmss(anyString());

				//----------------------------------------------------------------------------
				// second condition in method
				Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
				field.setAccessible(true);

				ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

				mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
				doNothing().when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

				assertDoesNotThrow(()->ClusterUtil.pushEngineSmss("engineId"));
			}
		}

		@Test
		public void testPushEngineSmssFail() throws Exception {
			//first condition in method
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);){

				mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
				doThrow(IOException.class).when(mockedCCS).pushEngineSmss(anyString());

				SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngineSmss("engineId"));
				assertEquals("Failed to push engine 'engineId'smss to cloud storage" ,e.getMessage());

				//----------------------------------------------------------------------------

				// second condition in method
				Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
				field.setAccessible(true);
//
//				// remove final modifier
//				Field modifiersField = Field.class.getDeclaredField("modifiers");
//				modifiersField.setAccessible(true);
//				modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
//
//				// set new value
//				field.set(ClusterUtil.IS_CLUSTER_ZK, true);

				doNothing().when(mockedCCS).pushEngineSmss(anyString());

				ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);
				mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);

				doThrow(IOException.class).when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

				SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngineSmss("engineId"));
				assertEquals("Failed to publish engine 'engineId' to sync with ZK cluster" ,e2.getMessage());
			}

		}

		@Test
		public void testPushEngineSmssSuccessTwoArg() throws Exception {
			//first condition in method
			try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);
					){
				CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
				mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
				doNothing().when(mockedCCS).pushEngineSmss(anyString(), any(IEngine.CATALOG_TYPE.class));

				//----------------------------------------------------------------------------
				// second condition in method
				Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
				field.setAccessible(true);

				ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

				mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
				doNothing().when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

				assertDoesNotThrow(()->ClusterUtil.pushEngineSmss("engineId", IEngine.CATALOG_TYPE.DATABASE));
			}
		}

		@Test
		public void testPushEngineSmssFailTwoArg() throws Exception {
			//first condition in method
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
					MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);){

				mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
				doThrow(IOException.class).when(mockedCCS).pushEngineSmss(anyString(),  any(IEngine.CATALOG_TYPE.class));

				SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngineSmss("engineId", IEngine.CATALOG_TYPE.DATABASE));
				assertEquals("Failed to push engine 'engineId'smss to cloud storage" ,e.getMessage());

				//----------------------------------------------------------------------------

				// second condition in method
				Field field = ClusterUtil.class.getDeclaredField("IS_CLUSTER_ZK");
				field.setAccessible(true);

//				// remove final modifier
//				Field modifiersField = Field.class.getDeclaredField("modifiers");
//				modifiersField.setAccessible(true);
//				modifiersField.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
//
//				// set new value
//				field.set(ClusterUtil.IS_CLUSTER_ZK, true);

				doNothing().when(mockedCCS).pushEngineSmss(anyString(), any(IEngine.CATALOG_TYPE.class));

				ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);
				mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);

				doThrow(IOException.class).when(mockedCS).publishEngineChange(anyString(), anyString(), any(Object.class));

				SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngineSmss("engineId", IEngine.CATALOG_TYPE.DATABASE));
				assertEquals("Failed to publish engine 'engineId' to sync with ZK cluster" ,e2.getMessage());
			}
		}
	}

	@Test
	public void testDeleteEngine() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).deleteEngine(anyString());
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("engineId"));
		}
	}
	
	@Test
	public void testDeleteEngineFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doThrow(IOException.class).when(mockedCCS).deleteEngine(anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteEngine("engineId"));
			assertEquals("Failed to delete engine 'engineId' from cloud storage" ,e2.getMessage());
		}
	}
	
	@Test
	public void testDeleteEngineTwoArgs() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).deleteEngine(anyString(), any(IEngine.CATALOG_TYPE.class));
			assertDoesNotThrow(() -> ClusterUtil.deleteEngine("engineId", IEngine.CATALOG_TYPE.DATABASE));
		}
	}
	
	@Test
	public void testDeleteEngineTwoArgsFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doThrow(IOException.class).when(mockedCCS).deleteEngine(anyString(), any(IEngine.CATALOG_TYPE.class));

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteEngine("engineId", IEngine.CATALOG_TYPE.DATABASE));
			assertEquals("Failed to delete engine 'engineId' from cloud storage" ,e2.getMessage());
		}
	}
	
	@Test
	public void testCopyLocalFilesToEngineCloudFolder() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).copyLocalFileToEngineCloudFolder(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.copyLocalFileToEngineCloudFolder("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
		}
	}
	
	@Test
	public void testCopyLocalFilesToEngineCloudFolderFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).copyLocalFileToEngineCloudFolder(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.copyLocalFileToEngineCloudFolder("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
			assertEquals("Failed to copy local file to engine 'engineId' storage" ,e2.getMessage());
		}
	}
	
	@Test
	public void testCopyEngineCloudFileToLocalFile() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).copyEngineCloudFileToLocalFile(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.copyEngineCloudFileToLocalFile("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
		}
	}
	
	@Test
	public void testCopyEngineCloudFileToLocalFileFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).copyEngineCloudFileToLocalFile(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.copyEngineCloudFileToLocalFile("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
			assertEquals("Failed to copy storage file from engine 'engineId' to local instance" ,e2.getMessage());
		}
	}
	
	@Test
	public void testDeleteEngineCloudFile() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).deleteEngineCloudFile(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineCloudFile("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
		}
	}
	
	@Test
	public void testDeleteEngineCloudFileFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).deleteEngineCloudFile(anyString(), any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteEngineCloudFile("engineId", IEngine.CATALOG_TYPE.DATABASE, "filePath"));
			assertEquals("Failed to delete storage file in engine 'engineId'",e2.getMessage());
		}
	}
	
	@Test
	public void testPullEngineAndProjectImageFolder() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pullEngineAndProjectImageFolder(any(IEngine.CATALOG_TYPE.class));
			assertDoesNotThrow(() -> ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE));
		}
	}
	
	@Test
	public void testPullEngineAndProjectImageFolderFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).pullEngineAndProjectImageFolder(any(IEngine.CATALOG_TYPE.class));

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullEngineAndProjectImageFolder(IEngine.CATALOG_TYPE.DATABASE));
			assertEquals("Failed to pull database image folder to cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testPushEngineAndProjectImage() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pushEngineAndProjectImage(any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "fileName"));
		}
	}
	
	@Test
	public void testPushEngineAndProjectImageFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).pushEngineAndProjectImage(any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "fileName"));
			assertEquals("Failed to push database image to cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testDeleteEngineAndProjectImage() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).deleteEngineAndProjectImage(any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "fileName"));
		}
	}
	
	@Test
	public void testDeleteEngineAndProjectImageFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).deleteEngineAndProjectImage(any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteEngineAndProjectImage(IEngine.CATALOG_TYPE.DATABASE, "fileName"));
			assertEquals("Failed to delete database image from the cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testDeleteEngineAndProjectImageById() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).deleteEngineAndProjectImageById(any(IEngine.CATALOG_TYPE.class), anyString());
			assertDoesNotThrow(() -> ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "engineId"));
		}
	}
	
	@Test
	public void testDeleteEngineAndProjectImageByIdFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).deleteEngineAndProjectImageById(any(IEngine.CATALOG_TYPE.class), anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteEngineAndProjectImageById(IEngine.CATALOG_TYPE.DATABASE, "engineId"));
			assertEquals("Failed to delete engine/project image from the cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testPullProject() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pullProject(anyString());
			assertDoesNotThrow(() -> ClusterUtil.pullProject("projectId"));
		}
	}
	
	@Test
	public void testPullProjectFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).pullProject(anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullProject("projectId"));
			assertEquals("Failed to pull project 'projectId' from cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testPullProjectTwoArgs() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pullProject(anyString(), anyBoolean());
			assertDoesNotThrow(() -> ClusterUtil.pullProject("projectId", false));
		}
	}
	
	@Test
	public void testPullProjectTwoArgsFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).pullProject(anyString(), anyBoolean());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullProject("projectId", false));
			assertEquals("Failed to pull project 'projectId' from cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testDeleteProject() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).deleteProject(anyString());
			assertDoesNotThrow(() -> ClusterUtil.deleteProject("projectId"));
		}
	}
	
	@Test
	public void testDeleteProjectFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).deleteProject(anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.deleteProject("projectId"));
			assertEquals("Failed to delete project 'projectId' from cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testPullInsightsDB() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pullInsightsDB(anyString());
			assertDoesNotThrow(() -> ClusterUtil.pullInsightsDB("projectId"));
		}
	}
	
	@Test
	public void testPullInsightsDBFail() throws Exception {
		// first condition in method
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doThrow(IOException.class).when(mockedCCS).pullInsightsDB(anyString());

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pullInsightsDB("projectId"));
			assertEquals("Failed to pull project 'projectId' insight database from cloud storage",e2.getMessage());
		}
	}
	
	@Test
	public void testPushInsightDB() throws Exception {
		//first condition in method
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
				MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);
				){
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doNothing().when(mockedCCS).pushInsightDB(anyString());

			//----------------------------------------------------------------------------
			// second condition in method

			ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

			mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
			doNothing().when(mockedCS).publishProjectChange(anyString(), anyString(), anyString());

			assertDoesNotThrow(()->ClusterUtil.pushInsightDB("projectId"));
		}
	}
	
	@Test
	public void testPushInsightDBFail() throws Exception{
		//first condition in method
		CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
		try(MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
				MockedStatic<ClusterSynchronizer> mockedStaticCS = Mockito.mockStatic(ClusterSynchronizer.class);){

			mockedStaticCCS.when(()-> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			doThrow(IOException.class).when(mockedCCS).pushInsightDB(anyString());

			SemossPixelException e = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushInsightDB("projectId"));
			assertEquals("Failed to push project 'projectId' insight database to cloud storage",e.getMessage());

			//----------------------------------------------------------------------------

			doNothing().when(mockedCCS).pushInsightDB(anyString());

			ClusterSynchronizer mockedCS = mock(ClusterSynchronizer.class);

			mockedStaticCS.when(()-> ClusterSynchronizer.getInstance()).thenReturn(mockedCS);
			doThrow(IOException.class).when(mockedCS).publishProjectChange(anyString(), anyString(), any(Object.class));

			SemossPixelException e2 = assertThrows(SemossPixelException.class, ()->ClusterUtil.pushInsightDB("projectId"));
			assertEquals("Failed to publish project 'projectId' to sync with ZK cluster to pull insight db" ,e2.getMessage());
		}
	}
	
	@Test
	public void testPullOwl() throws IOException, InterruptedException {
		try (MockedStatic<CentralCloudStorage> mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);) {
			CentralCloudStorage mockedCCS = mock(CentralCloudStorage.class);
			mockedStaticCCS.when(() -> CentralCloudStorage.getInstance()).thenReturn(mockedCCS);
			
			doNothing().when(mockedCCS).pullOwl(anyString(), any(WriteOWLEngine.class));

			assertDoesNotThrow(() -> ClusterUtil.pullOwl("projectId", null));
		}
	}
	
}
