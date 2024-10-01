package prerna.testing.reactor.database.upload.rdbms.csv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.reactor.database.upload.UploadTestUtility;
import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsUploadTableDataReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testUploadMovies() {
		// upload file
		String delimiter = ApiTestsSemossConstants.DELIMITER;
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		UploadTestUtility.uploadFile(filePath.toString());

		// run pixel
		Map<String, Object> predictedTypes = UploadTestUtility
				.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter);
		Map<String, Object> dataTypes = (Map<String, Object>) predictedTypes.get("dataTypes");
		boolean exists = false;
		String databaseName = "MOV_DB";
		Map<String, Object> dbInfo = UploadTestUtility.rdbmsUploadTable(databaseName,
				ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter, dataTypes, exists);
		
		// test output
		assertEquals(CATALOG_TYPE.DATABASE.toString(), dbInfo.get("database_type"));
		assertEquals(databaseName, dbInfo.get("database_name"));
		assertEquals(databaseName.toLowerCase(), dbInfo.get("low_database_name"));
		assertEquals("$", dbInfo.get("database_cost"));
		String dbId = (String) dbInfo.get("database_id");
		assertTrue(dbId != null && !dbId.isEmpty());
		assertFalse((boolean) dbInfo.get("database_discoverable"));
		assertFalse((boolean) dbInfo.get("database_global"));
		assertEquals(ApiTestsSemossConstants.USER_NAME, dbInfo.get("database_created_by"));
		assertEquals(RdbmsTypeEnum.H2_DB.getLabel(), dbInfo.get("database_subtype"));
		assertEquals("Native", dbInfo.get("database_created_by_type"));
	}

}
