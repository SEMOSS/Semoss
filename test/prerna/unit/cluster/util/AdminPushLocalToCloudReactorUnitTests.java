package prerna.unit.cluster.util;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
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
import prerna.cluster.util.clients.CentralCloudStorage;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.NounStore;

public class AdminPushLocalToCloudReactorUnitTests {

	private AdminPushLocalToCloudReactor spiedReactor;
	private Insight insight;
	private User user;
	private SelectQueryStruct qs;
	private NounStore ns;
	private CentralCloudStorage mockedCCS;
	private MockedStatic<SecurityAdminUtils> mockedStaticSAU ;
	private MockedStatic<CentralCloudStorage> mockedStaticCCS;
	@BeforeEach
	void setup() {
		spiedReactor =  spy(new AdminPushLocalToCloudReactor());

		insight = mock(Insight.class);
		user = mock(User.class);
		qs = mock(SelectQueryStruct.class);
		ns = mock(NounStore.class);
		mockedCCS = mock(CentralCloudStorage.class);

		spiedReactor.setNounStore(ns);
		spiedReactor.setInsight(insight);

		when(insight.getUser()).thenReturn(user);

		mockedStaticSAU = Mockito.mockStatic(SecurityAdminUtils.class);
		mockedStaticCCS = Mockito.mockStatic(CentralCloudStorage.class);
	}
	@AfterEach
	void tearDown() {
		// Close static mocks
		if (mockedStaticSAU != null) {
			mockedStaticSAU.close();
		}
		if (mockedStaticCCS != null) {
			mockedStaticCCS.close();
		}
	}

	@Test
	public void testUserNotAdmin() {

		mockedStaticSAU.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(false);

		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, spiedReactor::execute);
		assertEquals("User must be an admin for this operation!", e.getMessage());

	}

	@Test
	public void testExecuteFailure() throws Exception {
		// mock static methods

		mockedStaticSAU.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
		// Define the behavior to throw an exception

		mockedStaticCCS.when(CentralCloudStorage::getInstance).thenThrow(new RuntimeException("Error occurred"));
		Map<String, Object> nmd = (Map<String, Object>)spiedReactor.execute().getValue();
		// Verify that the exception is thrown
		assertEquals(1, nmd.size());

	}

	@Test
	public void testExecuteSuccess() throws Exception {
		try(MockedStatic<SecurityEngineUtils> mockedStaticSEU = Mockito.mockStatic(SecurityEngineUtils.class)){

			// mock static methods
			mockedStaticSAU.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			// Define the behavior to throw an exception

			mockedStaticCCS.when(CentralCloudStorage::getInstance).thenReturn(mockedCCS);

			List<String> cloudFiles = new ArrayList<>(Arrays.asList("value1", "value2/", "value3-smss/"));
			Map<String, List<String>> map = new HashMap<String, List<String>>();

			map.put(CentralCloudStorage.DATABASE_BLOB, cloudFiles);
			map.put(CentralCloudStorage.STORAGE_BLOB, cloudFiles);
			map.put(CentralCloudStorage.MODEL_BLOB, cloudFiles);
			map.put(CentralCloudStorage.VECTOR_BLOB, cloudFiles);
			map.put(CentralCloudStorage.FUNCTION_BLOB, cloudFiles);
			map.put(CentralCloudStorage.PROJECT_BLOB, cloudFiles);
			Mockito.when(mockedCCS.listAllContainersByBucket()).thenReturn(map);

			mockedStaticSEU.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(Arrays.asList("Test ID"));

			Map<String, Object> nmd = (Map<String, Object>)spiedReactor.execute().getValue();
			// ensure values were added as expected
			assertEquals(13, nmd.size());
		}
	}


	/**
	 * test removeExistingIds() method, value2 should be removed from startingList since it ends with a '/' in the cloudFiles list
	 * @throws Exception
	 */
	@Test
	public void testRemoveExistingIds() throws Exception {
		try(MockedStatic<SecurityEngineUtils> mockedStaticSEU = Mockito.mockStatic(SecurityEngineUtils.class)){

			// mock static methods
			mockedStaticSAU.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			// Define the behavior to throw an exception

			mockedStaticCCS.when(CentralCloudStorage::getInstance).thenReturn(mockedCCS);

			List<String> cloudFiles = new ArrayList<>(Arrays.asList("value1", "value2/", "value3-smss/"));
			Map<String, List<String>> map = new HashMap<String, List<String>>();

			map.put(CentralCloudStorage.DATABASE_BLOB, cloudFiles);
			map.put(CentralCloudStorage.STORAGE_BLOB, cloudFiles);
			map.put(CentralCloudStorage.MODEL_BLOB, cloudFiles);
			map.put(CentralCloudStorage.VECTOR_BLOB, cloudFiles);
			map.put(CentralCloudStorage.FUNCTION_BLOB, cloudFiles);
			map.put(CentralCloudStorage.PROJECT_BLOB, cloudFiles);
			Mockito.when(mockedCCS.listAllContainersByBucket()).thenReturn(map);


			mockedStaticSEU.when(() -> SecurityEngineUtils.getAllEngineIds(anyList())).thenReturn(new ArrayList<>(Arrays.asList("value1", "value2", "value3-smss/")));

			Map<String, Object> pushedChangesMap = (Map<String, Object>)spiedReactor.execute().getValue();

			// checking to make sure the removeExisitngIds method removed 'value2'
			assertFalse(((List<String>)pushedChangesMap.get("added_dbIds")).contains("value2"));
			assertFalse(((List<String>)pushedChangesMap.get("added_storageIds")).contains("value2"));
			assertFalse(((List<String>)pushedChangesMap.get("added_modelIds")).contains("value2"));
			assertFalse(((List<String>)pushedChangesMap.get("added_vectorIds")).contains("value2"));
			assertFalse(((List<String>)pushedChangesMap.get("added_functionIds")).contains("value2"));

			// Verify that the exception is thrown
			assertEquals(13, pushedChangesMap.size());

		}
	}
}
