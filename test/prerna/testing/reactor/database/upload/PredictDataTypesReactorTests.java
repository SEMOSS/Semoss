package prerna.testing.reactor.database.upload;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestInsightUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiTestsSemossConstants;

public class PredictDataTypesReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testPredictTypes() throws IOException {
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		
		// copy file to insight folder
		// TODO use a reactor to do this and test output
		File movieFile = new File(filePath.toString());
		File insightFolder = new File(ApiSemossTestInsightUtils.getInsight().getInsightFolder());
        FileUtils.copyFileToDirectory(movieFile, insightFolder);

		Map<String, Object> predictedTypes = UploadTestUtility.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME);
		ApiSemossTestUtils.print(predictedTypes);
		
		String[] headers = (String[]) predictedTypes.get("headers");
		Assert.assertArrayEquals(new String[] { "Nominated", "Title", "Genre", "Studio", "Director", "Revenue-Domestic",
				"MovieBudget", "Revenue-International", "RottenTomatoes-Critics", "RottenTomatoes-Audience" }, headers);

		String[] cleanHeaders = (String[]) predictedTypes.get("cleanHeaders");
		Assert.assertArrayEquals(new String[] { "Nominated", "Title", "Genre", "Studio", "Director", "Revenue_Domestic",
				"MovieBudget", "Revenue_International", "RottenTomatoes_Critics", "RottenTomatoes_Audience" }, cleanHeaders);
		
		Map<String, Object> dataTypes =  (Map<String, Object>) predictedTypes.get("dataTypes");
		// string cols
		assertEquals(SemossDataType.STRING.toString(), dataTypes.get(ApiTestsSemossConstants.STUDIO));
		assertEquals(SemossDataType.STRING.toString(), dataTypes.get(ApiTestsSemossConstants.GENRE));
		assertEquals(SemossDataType.STRING.toString(), dataTypes.get(ApiTestsSemossConstants.DIRECTOR));
		assertEquals(SemossDataType.STRING.toString(), dataTypes.get(ApiTestsSemossConstants.TITLE));
		assertEquals(SemossDataType.STRING.toString(), dataTypes.get(ApiTestsSemossConstants.NOMINATED));

		// int cols
		assertEquals(SemossDataType.INT.toString(), dataTypes.get(ApiTestsSemossConstants.REVENUE_DOMESTIC));
		assertEquals(SemossDataType.INT.toString(), dataTypes.get(ApiTestsSemossConstants.REVENUE_INTERNATIONAL));
		assertEquals(SemossDataType.INT.toString(), dataTypes.get(ApiTestsSemossConstants.MOVIE_BUDGET));

		// double cols
		assertEquals(SemossDataType.DOUBLE.toString(), dataTypes.get(ApiTestsSemossConstants.ROTTEN_TOMATOES_CRITICS));
		assertEquals(SemossDataType.DOUBLE.toString(), dataTypes.get(ApiTestsSemossConstants.ROTTEN_TOMATOES_AUDIENCE));

	}

}
