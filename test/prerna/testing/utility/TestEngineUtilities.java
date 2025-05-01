package prerna.testing.utility;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import prerna.reactor.security.SetEngineMetadataReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class TestEngineUtilities {
	
	public static void setEngineMetadata(String engineId, Map<String, Object> metaMap) {
		String addMetaPixel = ApiSemossTestUtils.buildPixelCall(SetEngineMetadataReactor.class, ReactorKeysEnum.ENGINE.getKey(),
				engineId, "meta", metaMap, ReactorKeysEnum.ENCODED.getKey(), false, ReactorKeysEnum.JSON_CLEANUP.getKey(), false);
		NounMetadata metaPixelCall = ApiSemossTestUtils.processPixel(addMetaPixel);
		assertTrue(Boolean.valueOf(metaPixelCall.getValue().toString()));
	}

}
