package prerna.testing.reactor.imports;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.qs.selectors.SelectTableReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.reactor.qs.source.FileReadReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.PixelChain;
import prerna.testing.PixelQueryTestUtils;
import prerna.testing.reactor.database.upload.UploadTestUtility;

/**
 * Test utility to import data into a frame
 */
public class ImportTestUtility {
	
	public static String uploadMovieDB(String databaseName) {
		String delimiter = ApiTestsSemossConstants.DELIMITER;
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		UploadTestUtility.uploadFile(filePath.toString());

		Map<String, Object> predictedTypes = UploadTestUtility
				.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter);
		Map<String, Object> dataTypes = (Map<String, Object>) predictedTypes.get("dataTypes");
		boolean exists = false;
		Map<String, Object> dbInfo = UploadTestUtility.rdbmsUploadTable(databaseName,
				ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter, dataTypes, exists);
		String databaseId = (String) dbInfo.get("database_id");
		return databaseId;
	}

	/**
	 * Import movie dataset from csv into frame
	 * @param frameType
	 * @param frameAlias
	 * @return
	 */
	public static ITableDataFrame fileReadMovie(String frameType, String frameAlias) {
		String delimiter = ApiTestsSemossConstants.DELIMITER;
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		UploadTestUtility.uploadFile(filePath.toString());
		
		Map<String, Object> predictedTypes = UploadTestUtility
				.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter);
		Map<String, Object> dataTypes = (Map<String, Object>) predictedTypes.get("dataTypes");
		Map<String, Object> newHeaders = new HashMap<>();
		Map<String, Object> addDataTypes = new HashMap<>();

		PixelChain fileRead = new PixelChain(fileRead(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, delimiter, newHeaders, dataTypes, addDataTypes ) );
		String dnd = "DND";
		int colSize = ApiTestsSemossConstants.MOVIE_TABLE_COLUMNS.size();
		String[] cols = new String[colSize];
		String[] alias = new String[colSize];
		for (int i = 0; i < colSize; i++) {
			cols[i] = dnd + "__" + ApiTestsSemossConstants.MOVIE_TABLE_COLUMNS.get(i);
			alias[i] = ApiTestsSemossConstants.MOVIE_TABLE_COLUMNS.get(i);
		}
		PixelChain select = PixelQueryTestUtils.select(cols, alias);
		String framePixel = PixelQueryTestUtils.createFramePixel(frameType, frameAlias, true);
		PixelChain importPC = PixelQueryTestUtils.importPixel(framePixel);
		
		String pixel = ApiSemossTestUtils.buildPixelChain(fileRead, select, importPC);
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		ITableDataFrame frame = (ITableDataFrame) noun.getValue();
		return frame;
	}

	/**
	 * 
	 * @param frameType
	 * @param frameAlias
	 * @param override
	 * @return
	 */
	public static ITableDataFrame createMovieFrame(String databaseId, String frameType, String frameAlias, boolean override) {
		String framePixel = PixelQueryTestUtils.createFramePixel(frameType, frameAlias, override);
		return runImportMoviePixels(databaseId, framePixel);
	}

	/**
	 * Util to load data into frame without a database
	 * 
	 * @param frameType
	 * @param frameAlias
	 * @param override
	 * @return
	 */
	public static String fileRead(String filePath, String delimiter, Map<String, Object> newHeaders,
			Map<String, Object> dataTypes, Map<String, Object> additionalDataTypes) {
		String fileName = new File(filePath).getName();
		String framePixel = ApiSemossTestUtils.buildPixelCall(FileReadReactor.class, ReactorKeysEnum.FILE_PATH.getKey(),
				filePath, ReactorKeysEnum.FILE_NAME.getKey(), fileName, ReactorKeysEnum.DELIMITER.getKey(), delimiter,
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey(), additionalDataTypes,
				ReactorKeysEnum.NEW_HEADER_NAMES.getKey(), newHeaders);
		framePixel = framePixel.replace(";", "");
		return framePixel;
	}


	private static ITableDataFrame runImportMoviePixels(String databaseId, String framePixel) {
		// remove semicolon
		// pixels to import all movie data into frame
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), databaseId);
		PixelChain select = new PixelChain(SelectTableReactor.class, ReactorKeysEnum.TABLE.getKey(),
				ApiTestsSemossConstants.MOVIE_TABLE_NAME);
		PixelChain importPixel = PixelQueryTestUtils.importPixel(framePixel);
		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, importPixel);
		NounMetadata noun = ApiSemossTestUtils.processPixel(pixel);
		ITableDataFrame frame = (ITableDataFrame) noun.getValue();
		return frame;
	}

}
