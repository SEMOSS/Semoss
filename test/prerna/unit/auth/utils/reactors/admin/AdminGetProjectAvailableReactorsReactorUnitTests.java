package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.reactors.admin.AdminGetProjectAvailableReactorsReactor;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.util.Utility;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetProjectAvailableReactorsReactorUnitTests {
	
	private AdminGetProjectAvailableReactorsReactor reactor;
	private Insight insight;
	private User user;
	
	private Map<String, String> keyValues;
	
	@BeforeEach
	void setup() {
		reactor = new AdminGetProjectAvailableReactorsReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
		
		keyValues = reactor.keyValue;
	}
	
	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
		//when(sau.apply(user)).thenReturn(null);
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
	void testExecuteNoReactorsReturned() {
		keyValues.put(ReactorKeysEnum.PROJECT.getKey(), "test");
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class);
				MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

				SecurityAdminUtils s = new SecurityAdminUtils();
				sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
				spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, "test")).thenReturn("testy");
			
				String projectId = "test";
				IProject project = mock(IProject.class);
				util.when(() -> Utility.getProject(projectId)).thenReturn(project);

				TreeSet<String> emptyTreeSet = new TreeSet<>();
				
				//this test is not fully working
				when(project.getAvailableReactors()).thenReturn(emptyTreeSet);
				NounMetadata nm = reactor.execute();
				assertEquals(emptyTreeSet, nm.getValue().toString());
				assertEquals(PixelDataType.CONST_STRING, nm.getNounType());

				
				spu.verify(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, "test"), times(1));
				util.verify(() -> Utility.getProject(projectId), times (1));
		}
	}
}
