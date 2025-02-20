package prerna.testing.utility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import prerna.project.api.IProject;
import prerna.reactor.project.CreateProjectReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class TestProjectUtils {

	@SuppressWarnings("unchecked")
	public static String createBasicProject(String name) {
		String pixel = ApiSemossTestUtils.buildPixelCall(CreateProjectReactor.class, ReactorKeysEnum.PROJECT.getKey(),
				name, ReactorKeysEnum.PROJECT_TYPE.getKey(), IProject.PROJECT_TYPE.INSIGHTS,
				ReactorKeysEnum.GLOBAL.getKey(), true, ReactorKeysEnum.PORTAL.getKey(), false);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertEquals(PixelDataType.UPLOAD_RETURN_MAP, nm.getNounType());
		Map<String, Object> retMap = (Map<String, Object>) nm.getValue();
		return retMap.get("project_id").toString();
	}
}
