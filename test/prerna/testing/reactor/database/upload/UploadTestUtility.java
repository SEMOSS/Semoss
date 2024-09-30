package prerna.testing.reactor.database.upload;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;

import prerna.reactor.database.upload.PredictDataTypesReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class UploadTestUtility {

	public static Map<String, Object> predictDataTypes(String filePath) {
		String pixel = ApiSemossTestUtils.buildPixelCall(PredictDataTypesReactor.class,
				ReactorKeysEnum.FILE_PATH.getKey(), filePath, ReactorKeysEnum.DELIMITER.getKey(), ",");
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retMap = (Map<String, Object>) noun.getValue();
		assertFalse(retMap.isEmpty());
		return retMap;
	}
}
