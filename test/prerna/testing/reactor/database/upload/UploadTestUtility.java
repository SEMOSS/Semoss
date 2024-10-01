package prerna.testing.reactor.database.upload;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Map;

import prerna.reactor.database.upload.PredictDataTypesReactor;
import prerna.reactor.database.upload.rdbms.csv.RdbmsUploadTableDataReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;

public class UploadTestUtility {

	public static Map<String, Object> predictDataTypes(String filePath, String delimiter) {
		String pixel = ApiSemossTestUtils.buildPixelCall(PredictDataTypesReactor.class,
				ReactorKeysEnum.FILE_PATH.getKey(), filePath, ReactorKeysEnum.DELIMITER.getKey(), delimiter);
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retMap = (Map<String, Object>) noun.getValue();
		assertFalse(retMap.isEmpty());
		return retMap;
	}

	public static Map<String, Object> rdbmsUploadTable(String databaseName, String filePath, String delimiter,
			Map<String, Object> dataTypeMap, boolean exists) {
		String pixel = ApiSemossTestUtils.buildPixelCall(RdbmsUploadTableDataReactor.class,
				ReactorKeysEnum.DATABASE.getKey(), databaseName, ReactorKeysEnum.FILE_PATH.getKey(), filePath,
				ReactorKeysEnum.DELIMITER.getKey(), delimiter, ReactorKeysEnum.DATA_TYPE_MAP.getKey(), dataTypeMap,
				ReactorKeysEnum.EXISTING.getKey(), exists);
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> retMap = (Map<String, Object>) noun.getValue();
		assertFalse(retMap.isEmpty());
		return retMap;
	}

}
