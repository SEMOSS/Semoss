package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminGetProjectPortalDetailsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestProjectUtils;

public class AdminGetProjectPortalDetailsReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void executeWithValidInput() {
		String project = TestProjectUtils.createBasicProject("testProject");

		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetProjectPortalDetailsReactor.class,
				ReactorKeysEnum.PROJECT.getKey(), project);

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);

		Map<String, Object> portalDetails = (Map<String, Object>) nm.getValue();
		assertNotNull(nm);
		assertEquals(PixelDataType.MAP, nm.getNounType());
		assertFalse(Boolean.valueOf(portalDetails.get("project_has_portal").toString()));
		assertFalse(Boolean.valueOf(portalDetails.get("project_is_published").toString()));
		assertFalse(Boolean.valueOf(portalDetails.get("isPublished").toString()));
	}
	

}
