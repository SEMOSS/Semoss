package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.reactors.admin.AdminGetProjectPortalDetailsReactor;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetProjectPortalDetailsReactorUnitTests {

	private AdminGetProjectPortalDetailsReactor reactor;
	private Insight insight;
	private User user;
	
	private Map<String, String> keyValues;
	
	@BeforeEach
	void setup() {
		reactor = new AdminGetProjectPortalDetailsReactor();
		keyValues= reactor.keyValue;

		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
	}
	
	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}
	
	@Test
	void testProjectIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an project id", e.getMessage());
		}
	}
	
	@Test
	void testProjectIdEmpty() {
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), "");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an project id", e.getMessage());
		}
	}
	
	@Test
	void testProjectId() throws IOException {
		String projectId = "test";
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class)){
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectId)).thenReturn("testy");
			
			Map<String, Object> portalDetails = new HashMap<>();
			portalDetails.put("foo", "bar");
			
			spu.when(() -> SecurityProjectUtils.getProjectPortalDetailsMap("testy")).thenReturn(portalDetails);
			NounMetadata nm = reactor.execute();
			Map<String, Object> resultMap = (Map<String, Object>) nm.getValue();
			assertEquals("bar", resultMap.get("foo"));
			assertEquals(PixelDataType.MAP, nm.getNounType());
			
			spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, "test"), times(1));
			spu.verify(() -> SecurityProjectUtils.getProjectPortalDetailsMap("testy"), times(1));
		}
	}
	
}
