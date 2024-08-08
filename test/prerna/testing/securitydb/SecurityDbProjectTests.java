package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.project.api.IProject;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbProjectTests extends AbstractBaseSemossApiTests {

	private static final String PERMISSION_TEST_PROJECTID = "testing-projectid";

	@BeforeAll
    public static void initialSetup() throws Exception {
		AbstractBaseSemossApiTests.initialSetup();
		// unnecessary if running by itself, but necessary if running {@link prerna.testing.AllTests}
		ApiSemossTestEngineUtils.deleteAllDataAndAddUser();
	}
	
	@Override
	@BeforeEach
	public void beforeEachTest() throws Exception {
		this.clearAllDatabasesBetweenTests = false;
		super.beforeEachTest();
	}
	
	@Test
	@Order(1)
	public void addProject() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			SecurityProjectUtils.addProject(
					PERMISSION_TEST_PROJECTID, 
					PERMISSION_TEST_PROJECTID,
					IProject.PROJECT_TYPE.INSIGHTS.name(),
					"",
					false, 
					null,
					false,
					defaultTestAdminUser);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
}
