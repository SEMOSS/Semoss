package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.auth.utils.SecurityNativeUserUtils;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbGroupTests extends AbstractBaseSemossApiTests {

	private static final String TEST_GROUP = "myNewGroup";
	private static final String TEST_GROUP_TYPE = null; // custom groups dont have a type
	private static final String TEST_GROUP_DESCRIPTION = "my description";
	private static final boolean TEST_GROUP_IS_CUSTOM = true;

	private static final String NATIVE_DUMMY_USER_ID = "DUMMY_USER_123";
	private static final AuthProvider NATIVE_DUMMY_USER_PROVIDER = AuthProvider.NATIVE;
	private static final String NATIVE_DUMMY_USERNAME = "DUMMYUSERNAME";
	private static final String NATIVE_DUMMY_EMAIL = "example@mail.com";
	private static final String NATIVE_DUMMY_PASSWORD = "SEMoss@123123!@#";

	@BeforeAll
    public static void initialSetup() throws Exception {
		AbstractBaseSemossApiTests.initialSetup();
		// unnecessary if running by itself, but necessary if running {@link prerna.testing.AllTests}
		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
	}
	
	@Override
	@BeforeEach
	public void beforeEachTest() {
		this.clearAllDatabasesBetweenTests = false;
		super.beforeEachTest();
	}

	@Test
	@Order(1)
	public void createGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, TEST_GROUP,
					TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, TEST_GROUP_IS_CUSTOM);
			assertTrue(true);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}

	@Test
	@Order(2)
	public void listAvailableGroups() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, -1);
			assertTrue(groupReturns.size() == 1);

			Map<String, Object> myGroup = groupReturns.get(0);
			assertTrue(myGroup.get("id").equals(TEST_GROUP));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
			assertTrue((boolean) myGroup.get("is_custom_group"));

			assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
			assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
			assertNotNull(myGroup.get("dateadded"));
		}
		// with a search term
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups("my", -1, -1);
			assertTrue(groupReturns.size() == 1);

			Map<String, Object> myGroup = groupReturns.get(0);
			assertTrue(myGroup.get("id").equals(TEST_GROUP));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
			assertTrue((boolean) myGroup.get("is_custom_group"));

			assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
			assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
			assertNotNull(myGroup.get("dateadded"));
		}
		// search not found
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups("z", -1, -1);
			assertTrue(groupReturns.isEmpty());
		}
		// offset is large
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, 10);
			assertTrue(groupReturns.isEmpty());
		}
	}
	
	@Test
	@Order(3)
	public void createGroupWithExistingName() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		assertThrows(IllegalArgumentException.class, 
				() -> {
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, TEST_GROUP,
							TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, TEST_GROUP_IS_CUSTOM);
					},
				"Group already exists"
				);
	}
	
	@Test
	@Order(4)
	public void editGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupAndPropagate(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP, TEST_GROUP_TYPE, "new description", true);
			
			{
				List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getGroups(null, -1, -1);
				assertTrue(groupReturns.size() == 1);

				Map<String, Object> myGroup = groupReturns.get(0);
				assertTrue(myGroup.get("description").equals("new description"));
				
				// check the other stuff as well
				assertTrue(myGroup.get("id").equals(TEST_GROUP));
				assertNull(myGroup.get("type"));
				assertTrue((boolean) myGroup.get("is_custom_group"));
				assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
				assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
				assertNotNull(myGroup.get("dateadded"));
			}
			
			// reset it back 
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.editGroupAndPropagate(defaultTestAdminUser, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP, TEST_GROUP_TYPE, TEST_GROUP_DESCRIPTION, true);
			{
				List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
						.getGroups(null, -1, -1);
				assertTrue(groupReturns.size() == 1);
				
				Map<String, Object> myGroup = groupReturns.get(0);
				assertTrue(myGroup.get("id").equals(TEST_GROUP));
				assertNull(myGroup.get("type"));
				assertTrue(myGroup.get("description").equals(TEST_GROUP_DESCRIPTION));
				assertTrue((boolean) myGroup.get("is_custom_group"));
				assertTrue(myGroup.get("userid").equals(userDetails.getValue0()));
				assertTrue(myGroup.get("useridtype").equals(userDetails.getValue1()));
				assertNotNull(myGroup.get("dateadded"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	@Order(5)
	public void addGroupMember() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		
		try {
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.addUserToGroup(defaultTestAdminUser, TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
						},
					"User " + NATIVE_DUMMY_USER_ID + " doesn't exist"
					);
			
			AccessToken newUser = new AccessToken();
			newUser.setId(NATIVE_DUMMY_USER_ID);
			newUser.setProvider(NATIVE_DUMMY_USER_PROVIDER);
			newUser.setUsername(NATIVE_DUMMY_USERNAME);
			newUser.setEmail(NATIVE_DUMMY_EMAIL);
			
			assertTrue(SecurityNativeUserUtils.addNativeUser(newUser, NATIVE_DUMMY_PASSWORD));

			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
				.addUserToGroup(defaultTestAdminUser, TEST_GROUP, NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
			
			
			assertThrows(IllegalArgumentException.class, 
					() -> {
						AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
							.addUserToGroup(defaultTestAdminUser, "GROUP_DOESN'T_EXIST", NATIVE_DUMMY_USER_ID, NATIVE_DUMMY_USER_PROVIDER.toString(), null);
						},
					"Group GROUP_DOESN'T_EXIST does not exist"
					);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	@Test
	@Order(6)
	public void listGroupMembers() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		Pair<String, String> userDetails = User.getPrimaryUserIdAndTypePair(defaultTestAdminUser);

		{
			List<Map<String, Object>> members = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).getGroupMembers(TEST_GROUP, null, -1, -1);
			assertTrue(members.size() == 1);
			
			Map<String, Object> member = members.get(0);
			assertTrue(member.get("groupid").equals(TEST_GROUP));
			assertTrue(member.get("userid").equals(NATIVE_DUMMY_USER_ID));
			assertTrue(member.get("type").equals(NATIVE_DUMMY_USER_PROVIDER.toString()));
			assertNull(member.get("datadded"));
			assertNull(member.get("enddate"));
			assertTrue(member.get("permissiongrantedby").equals(userDetails.getValue0()));
			assertTrue(member.get("permissiongrantedbytype").equals(userDetails.getValue1()));
		}
	}
	
	@Test
	@Order(999)
	public void deleteGroupMember() {
		
	}
	
	// perform at the end
	@Test
	@Order(1000)
	public void deleteGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).deleteGroupAndPropagate(TEST_GROUP, TEST_GROUP_TYPE);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
		
		// should have no groups left
		{
			List<Map<String, Object>> groupReturns = AdminSecurityGroupUtils.getInstance(defaultTestAdminUser)
					.getGroups(null, -1, -1);
			assertTrue(groupReturns.isEmpty());
		}
	}
}
