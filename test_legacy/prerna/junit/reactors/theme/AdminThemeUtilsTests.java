package prerna.junit.reactors.theme;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import prerna.auth.User;
import prerna.theme.AdminThemeUtils;

public class AdminThemeUtilsTests {
	
	@Test
	public void testGetInstance() {
		User user = new User();
		AdminThemeUtils instance = AdminThemeUtils.getInstance(user);
		assertNotNull(instance);
	}

}
