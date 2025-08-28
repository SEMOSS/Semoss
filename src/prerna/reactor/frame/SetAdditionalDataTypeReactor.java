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
package prerna.reactor.frame;

import java.util.ArrayList;
import java.util.List;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetAdditionalDataTypeReactor extends AbstractFrameReactor {

	public SetAdditionalDataTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FORMAT.getKey(), ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String format = getFormatType();
		ArrayList<String> columns = (ArrayList<String>) getColumnsToFormat();
		ITableDataFrame frame = getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// loop through columns passed and set additional data types
		for (String column : columns) {
			metaData.modifyAdditionalDataTypeToProperty(frame.getName() + "__" + column, frame.getName(), format);
		}

		return new NounMetadata(frame.getFrameHeadersObject(), PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.FRAME_HEADERS, PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////
  ///////////////////////// GET PIXEL INPUT ////////////////////////////
  //////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////

	private String getFormatType() {
		GenRowStruct formatInput = this.store.getNoun(ReactorKeysEnum.FORMAT.getKey());
		if (formatInput != null) {
			return formatInput.getNoun(0).getValue().toString();
		}
		throw new IllegalArgumentException("Need to define formatting for specified column(s)");
	}

	private List<String> getColumnsToFormat() {
		GenRowStruct columnValuesInput = this.store.getNoun(ReactorKeysEnum.COLUMNS.getKey());
		List<String> stringColumnValues = new ArrayList<>();
		if (columnValuesInput != null) {
			List<Object> columnValues = columnValuesInput.getAllValues();
			for (Object columnValue : columnValues) {
				stringColumnValues.add(columnValue.toString());
			}

			return stringColumnValues;
		}
		throw new IllegalArgumentException("Need to define columns to define formatting for");
	}
}
