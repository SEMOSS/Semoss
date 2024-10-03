package prerna.testing.reactor.imports;

import java.nio.file.Path;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.frame.CreateFrameReactor;
import prerna.reactor.qs.selectors.SelectTableReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.PixelChain;
import prerna.testing.reactor.database.upload.UploadTestUtility;

/**
 * Test utility to import data into a frame
 */
public class ImportTestUtility {
	
	
	public static ITableDataFrame createMovieFrame(String frameType, String frameAlias, boolean override) {
		String framePixel = ApiSemossTestUtils.buildPixelCall(CreateFrameReactor.class,
				ReactorKeysEnum.FRAME_TYPE.getKey(), frameType, "override", override, ReactorKeysEnum.ALIAS.getKey(),
				frameAlias);
		return runImportMoviePixels(framePixel);
	}
	
	private static ITableDataFrame runImportMoviePixels(String framePixel) {
		// remove semicolon
		framePixel = framePixel.replace(";", "");
		String databaseId = uploadMovieDB();
		// pixels to import all movie data into frame
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), databaseId);
		PixelChain select = new PixelChain(SelectTableReactor.class, ReactorKeysEnum.TABLE.getKey(), ApiTestsSemossConstants.MOVIE_TABLE_NAME);
		PixelChain importPixel = new PixelChain("Import(frame=["+framePixel+"])");
		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, importPixel );
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		ITableDataFrame frame  = (ITableDataFrame) noun.getValue();
		return frame;
	}
	
	private static String uploadMovieDB() {
		String delimiter = ApiTestsSemossConstants.DELIMITER;
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		UploadTestUtility.uploadFile(filePath.toString());
		
		Map<String, Object> predictedTypes = UploadTestUtility
				.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter);
		Map<String, Object> dataTypes = (Map<String, Object>) predictedTypes.get("dataTypes");
		boolean exists = false;
		String databaseName = "MOV_DB";
		Map<String, Object> dbInfo = UploadTestUtility.rdbmsUploadTable(databaseName,
				ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter, dataTypes, exists);
		String databaseId = (String) dbInfo.get("database_id");
		return databaseId;
	}

}
