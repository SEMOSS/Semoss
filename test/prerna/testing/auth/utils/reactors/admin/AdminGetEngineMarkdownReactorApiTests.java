package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminGetEngineMarkdownReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AdminGetEngineMarkdownReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void executeWithMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		Map<String, Object> map = new HashMap<>();
		map.put("markdown", "### test markdown");
		TestEngineUtilities.setEngineMetadata(engine, map);
		
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetEngineMarkdownReactor.class, ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		String retValue = (String) nm.getValue();
		assertEquals("### test markdown", retValue);
	}
	
	@Test
	public void executeWithoutMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();		
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetEngineMarkdownReactor.class, ReactorKeysEnum.ENGINE.getKey(), engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNull(nm.getValue());
	}
}