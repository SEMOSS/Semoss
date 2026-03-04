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
package prerna.reactor.frame.rdbms;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class EncodeColumnReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(EncodeColumnReactor.class);

	@Override
	public NounMetadata execute() {

		organizeKeys();

		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		Set<String> columnHeaders = Stream.of(frame.getColumnHeaders()).collect(Collectors.toSet());
		List<String> columns = this.store.nounRow.get("columns").getVector().stream()
				.map(noun -> noun.getValue().toString()).collect(Collectors.toList());
		if (!columnHeaders.containsAll(columns)) {
			throw new IllegalArgumentException(
					"One or more columns could not be found: " + columnHeaders.removeAll(columns));
		}

		String[] columnsToUpdate = new String[columns.size()];
		columns.toArray(columnsToUpdate);

		String frameName = frame.getName();
		PreparedStatement statement = frame.getBuilder().hashColumn(frameName, columnsToUpdate);
		try {
			for (int i = 0; i < columns.size(); i++) {
				String col = columns.get(i);
				String query = frame.getQueryUtil().modColumnType(frameName, col, "VARCHAR");
				frame.getBuilder().runQuery(query);
			}
			statement.execute();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// upon successful execution
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		for (String col : columns) {
			// set the type for all the columns to be string
			metadata.modifyDataTypeToProperty(frameName + "__" + col, frameName, SemossDataType.STRING.toString());
		}

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		return noun;
	}
}
