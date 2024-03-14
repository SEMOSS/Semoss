package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import java.util.Map;

import org.javatuples.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUserUtils;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbGroupTests extends AbstractBaseSemossApiTests {

	private static final String NEW_GROUP_ID = "myNewGroup";
	private static final String NEW_GROUP_TYPE = null; // custom groups dont have a type
	private static final String NEW_GROUP_DESCRIPTION = "my description";
	private static final boolean NEW_GROUP_IS_CUSTOM = true;

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
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, NEW_GROUP_ID,
					NEW_GROUP_TYPE, NEW_GROUP_DESCRIPTION, NEW_GROUP_IS_CUSTOM);
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
			assertTrue(myGroup.get("id").equals(NEW_GROUP_ID));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(NEW_GROUP_DESCRIPTION));
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
			assertTrue(myGroup.get("id").equals(NEW_GROUP_ID));
			assertNull(myGroup.get("type"));
			assertTrue(myGroup.get("description").equals(NEW_GROUP_DESCRIPTION));
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
					AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).addGroup(defaultTestAdminUser, NEW_GROUP_ID,
							NEW_GROUP_TYPE, NEW_GROUP_DESCRIPTION, NEW_GROUP_IS_CUSTOM);
					},
				"Group already exists"
				);
	}
	
	
	// perform at the end
	@Test
	@Order(1000)
	public void deleteGroup() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();
		try {
			AdminSecurityGroupUtils.getInstance(defaultTestAdminUser).deleteGroupAndPropagate(NEW_GROUP_ID, NEW_GROUP_TYPE);
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
