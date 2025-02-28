package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AdminExecQueryReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void executeWithoutMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(), 
				engine);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
	}

	
	@Test
	public void executeWithMetakeysExcludesMarkdown() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("markdown", "### test markdown");
		TestEngineUtilities.setEngineMetadata(engine, map);
		
		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("markdown");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(), 
				engine, ReactorKeysEnum.META_KEYS.getKey(), metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();

	}
	
	@Test
	public void executeWithMultipleMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();

	}
	
}
