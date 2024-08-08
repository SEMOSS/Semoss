package prerna.testing.securitydb;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUserUtils;
import prerna.util.sql.RdbmsTypeEnum;

@TestMethodOrder(OrderAnnotation.class)
public class SecurityDbEngineTests extends AbstractBaseSemossApiTests {

	private static final String PERMISSION_TEST_ENGINEID = "testing-engineid";

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
	public void addEngine() {
		User defaultTestAdminUser = ApiSemossTestUserUtils.getUser();

		try {
			SecurityEngineUtils.addEngine(
					PERMISSION_TEST_ENGINEID, 
					PERMISSION_TEST_ENGINEID, 
					IEngine.CATALOG_TYPE.DATABASE, 
					RdbmsTypeEnum.H2_DB.getLabel(), 
					"", 
					false, 
					defaultTestAdminUser);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
}
