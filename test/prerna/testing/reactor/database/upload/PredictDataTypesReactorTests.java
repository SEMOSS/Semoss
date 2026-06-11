/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.testing.reactor.database.upload;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiTestsSemossConstants;

public class PredictDataTypesReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testPredictTypes() {
		// upload file
		Path filePath = ApiTestsSemossConstants.TEST_MOVIE_CSV_PATH;
		UploadTestUtility.uploadFile(filePath.toString());

		// run pixel
		Map<String, Object> predictedTypes = UploadTestUtility.predictDataTypes(ApiTestsSemossConstants.MOVIE_CSV_FILE_NAME, ApiTestsSemossConstants.DELIMITER);
		
		// test output
		String[] headers = (String[]) predictedTypes.get("headers");
		assertArrayEquals(new String[] { "Nominated", "Title", "Genre", "Studio", "Director", "Revenue-Domestic",
				"MovieBudget", "Revenue-International", "RottenTomatoes-Critics", "RottenTomatoes-Audience" }, headers);

		String[] cleanHeaders = (String[]) predictedTypes.get("cleanHeaders");
		assertArrayEquals(new String[] { "Nominated", "Title", "Genre", "Studio", "Director", "Revenue_Domestic",
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
