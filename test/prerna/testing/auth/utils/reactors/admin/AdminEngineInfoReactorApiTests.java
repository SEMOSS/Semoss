package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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

public class AdminEngineInfoReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void executeWithoutMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(), 
				engine);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());
		
		// add assertions for values
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
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());
		
		
		// add assertions for values
		
		// add assertion that markdown was filtered out
		assertFalse(retValue.containsKey("markdown"));
		
		// add assertion for description
		
	}
	
	@Test
	public void executeWithMultipleMetakeys() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		Map<String, Object> map = new HashMap<>();
		map.put("description", "test description");
		map.put("domain", "test domain");
		TestEngineUtilities.setEngineMetadata(engine, map);
		
		List<String> metaValues = new ArrayList<>();
		metaValues.add("description");
		metaValues.add("domain");
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminEngineInfoReactor.class, ReactorKeysEnum.ENGINE.getKey(), 
				engine, ReactorKeysEnum.META_KEYS.getKey(), metaValues);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		Map<String, Object> retValue = (Map<String, Object>) nm.getValue();
		assertFalse(Boolean.valueOf(retValue.get("app_global").toString()));
		assertFalse(Boolean.valueOf(retValue.get("engine_global").toString()));
		assertEquals("test", retValue.get("database_name").toString());
		assertEquals(engine, retValue.get("engine_id").toString());
		assertEquals(engine, retValue.get("database_id").toString());
		
		
		// add assertions for values
		
		// add assertion for description
		// add assertion for domain
		
	}
	
}
