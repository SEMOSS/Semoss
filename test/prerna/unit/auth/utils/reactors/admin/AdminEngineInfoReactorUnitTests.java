package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.om.Insight;

public class AdminEngineInfoReactorUnitTests {

	private AdminEngineInfoReactor reactor;
	private Insight insight;
	private User user;

	@BeforeEach
	void setup() {
		reactor = new AdminEngineInfoReactor();
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
	void testEngineIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an engine id", e.getMessage());
		}
	}

	@Test
	void testEngineEmpty() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("engine", "");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an engine id", e.getMessage());
		}
	}
	
	@Test
	void testBaseInfoNull() {
		Map<String, String> keyvalues = reactor.keyValue;
        keyvalues.put("engine", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
			MockedStatic<SecurityQueryUtils> squ = Mockito.mockStatic(SecurityQueryUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			squ.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "test")).thenReturn("testid");
			
			List<Map<String, Object>> baseInfo = new ArrayList<>();
			when(s.getAllEngineSettings(any(List.class),  eq(null), eq(null), eq(null), eq(null), eq(null))).thenReturn(baseInfo);
			
			

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find any engine data", e.getMessage());
			
			ArgumentCaptor<List<String>> listCaptor = ArgumentCaptor.forClass(List.class);
			verify(s, times(1)).getAllEngineSettings(listCaptor.capture(), eq(null), eq(null), eq(null), eq(null), eq(null));
			assertEquals(1, listCaptor.getValue().size());
			assertTrue(listCaptor.getValue().contains("testid"));
			
			squ.verify(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "test"), times(1));
		}
	}
	
	
	@Test
	void testBaseInfoEmpty() {
		Map<String, String> keyvalues = reactor.keyValue;
        keyvalues.put("engine", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);	
		MockedStatic<SecurityQueryUtils> squ = Mockito.mockStatic(SecurityQueryUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			squ.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "test")).thenReturn("");
			
			//List<Map<String, Object>> testBaseInfo = mock(List.class);
			//sau.when(() -> SecurityAdminUtils.getAllEngineSettings(Arrays.asList("test"), null, null, null, null, null)).thenReturn(null);
			//when(testBaseInfo.getAllEngineSettings(Arrays.asList("test"), null, null, null, null, null)).thenReturn(null);
			
			
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Could not find any engine data", e.getMessage());
		}
	}
	
	
	@Test
	void testEngineInfo() {
		Map<String, String> keyvalues = reactor.keyValue;
		keyvalues.put("engine", "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
			MockedStatic<SecurityQueryUtils> squ = Mockito.mockStatic(SecurityQueryUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			squ.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "test")).thenReturn("");
			

			//NounMetadata nm = reactor.execute();
			// asserts
			// verifications
		}	
	}
}
