package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.reactors.admin.AdminGetEngineSMSSReactor;
import prerna.auth.utils.reactors.admin.AdminGetProjectPortalDetailsReactor;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetProjectPortalDetailsReactorUnitTests {

	private AdminGetProjectPortalDetailsReactor reactor;
	private Insight insight;
	private User user;
	private GenRowStruct projectGrs;
	
	@BeforeEach
	void setup() {
		reactor = new AdminGetProjectPortalDetailsReactor();
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
	void testProjectId() throws IOException {
		
//		when(projectGrs.isEmpty()).thenReturn(false);
//		when(projectGrs.get(0)).thenReturn("test");
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<SecurityProjectUtils> spu = Mockito.mockStatic(SecurityProjectUtils.class)){
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			
			String projectId = "test";
			spu.when(() -> SecurityProjectUtils.testUserProjectIdForAlias(user, projectId)).thenReturn("testy");
			
			Map<String, Object> portalDetails = new HashMap<String, Object>() {{
				put("test1","test2"); }};
			
			spu.when(() -> SecurityProjectUtils.getProjectPortalDetailsMap(projectId)).thenReturn(portalDetails);
			NounMetadata nm = reactor.execute();
			Map<String, Object> resultMap = (Map<String, Object>) nm.getValue();
			assertEquals("test2", resultMap.get("test1"));
			assertEquals(PixelDataType.MAP, nm.getNounType());
		}
	}
	
}
