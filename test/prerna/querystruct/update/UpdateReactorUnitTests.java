/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.querystruct.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.reactors.UpdateReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateReactorUnitTests {

	private UpdateReactor reactor;

	@Mock
	private NounStore mockStore;

	@Mock
	private GenRowStruct mockColGrs;

	@Mock
	private GenRowStruct mockValGrs;

	@BeforeEach
	void setup() {
		MockitoAnnotations.openMocks(this);
		reactor = new UpdateReactor();
		reactor.setNounStore(mockStore);
		when(mockStore.getNoun(ReactorKeysEnum.COLUMNS.getKey())).thenReturn(mockColGrs);
		when(mockStore.getNoun(ReactorKeysEnum.VALUES.getKey())).thenReturn(mockValGrs);
	}

	@Test
	void testExecuteWithValidColumnsAndValues() {
		when(mockColGrs.size()).thenReturn(2);
		when(mockColGrs.get(0)).thenReturn("column1");
		when(mockColGrs.get(1)).thenReturn("column2");
		when(mockValGrs.get(0)).thenReturn("value1");
		when(mockValGrs.get(1)).thenReturn("value2");

		NounMetadata result = null;
		result = reactor.execute();

		UpdateQueryStruct qs = (UpdateQueryStruct) result.getValue();
		List<IQuerySelector> expectedSelectors = Arrays.asList(new QueryColumnSelector("column1"),
				new QueryColumnSelector("column2"));
		List<Object> expectedValues = Arrays.asList("value1", "value2");

		assertEquals(expectedSelectors, qs.getSelectors());
		assertEquals(expectedValues, qs.getValues());
	}

	@Test
	void testExecuteWithListValues() {
		when(mockColGrs.size()).thenReturn(1);
		when(mockColGrs.get(0)).thenReturn("column1");
		when(mockValGrs.get(0)).thenReturn(Arrays.asList("value1"));

		NounMetadata result = reactor.execute();

		UpdateQueryStruct qs = (UpdateQueryStruct) result.getValue();
		List<IQuerySelector> expectedSelectors = Arrays.asList(new QueryColumnSelector("column1"));
		List<Object> expectedValues = Arrays.asList("value1");

		assertEquals(expectedSelectors, qs.getSelectors());
		assertEquals(expectedValues, qs.getValues());
	}

	@Test
	void testExecuteWithNounMetadataValues() {
		when(mockColGrs.size()).thenReturn(1);
		when(mockColGrs.get(0)).thenReturn("column1");
		NounMetadata nounMetadata = new NounMetadata("value1", PixelDataType.CONST_STRING);
		when(mockValGrs.get(0)).thenReturn(nounMetadata);

		NounMetadata result = reactor.execute();

		UpdateQueryStruct qs = (UpdateQueryStruct) result.getValue();
		List<IQuerySelector> expectedSelectors = Arrays.asList(new QueryColumnSelector("column1"));
		List<Object> expectedValues = Arrays.asList("value1");

		assertEquals(expectedSelectors, qs.getSelectors());
		assertEquals(expectedValues, qs.getValues());
	}

	@Test
	void testExecuteWithMultipleValuesInList() {
		when(mockColGrs.size()).thenReturn(1);
		when(mockColGrs.get(0)).thenReturn("column1");
		when(mockValGrs.get(0)).thenReturn(Arrays.asList("value1", "value2"));

		SemossPixelException exception = assertThrows(SemossPixelException.class, () -> {
			reactor.execute();
		});

		String expectedNounMetadata = "Can only specify one value to update to";
		assertEquals(expectedNounMetadata, exception.getMessage());
	}

	@Test
	void testExecuteWithNounMetadataInList() {
		when(mockColGrs.size()).thenReturn(1);
		when(mockColGrs.get(0)).thenReturn("column1");
		NounMetadata nounMetadata = new NounMetadata("value1", PixelDataType.CONST_STRING);
		when(mockValGrs.get(0)).thenReturn(Arrays.asList(nounMetadata));

		NounMetadata result = reactor.execute();

		UpdateQueryStruct qs = (UpdateQueryStruct) result.getValue();
		List<IQuerySelector> expectedSelectors = Arrays.asList(new QueryColumnSelector("column1"));
		List<Object> expectedValues = Arrays.asList("value1");

		assertEquals(expectedSelectors, qs.getSelectors());
		assertEquals(expectedValues, qs.getValues());
	}
}
