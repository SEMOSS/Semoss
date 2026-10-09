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
package prerna.reactor.imports.union;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PolarsFrame;
import prerna.om.Insight;

/**
 * Performs schema-aligned union operations directly between Polars frames.
 * Column names, order, and dtypes must match; callers must rename or convert
 * columns before invoking the union.
 */
public class PolarsUnion extends AbstractUnion {

	@Override
	public ITableDataFrame performUnion(ITableDataFrame a, ITableDataFrame b, String unionType, Insight insight,
			Logger logger) {
		if (!(a instanceof PolarsFrame) || !(b instanceof PolarsFrame)) {
			throw new IllegalArgumentException("Polars union requires two Polars frames");
		}
		List<String> aColumns = getSemossCols(a.getQsHeaders());
		List<String> bColumns = getSemossCols(b.getQsHeaders());
		checkColNames(aColumns, bColumns);
		((PolarsFrame) a).unionWith((PolarsFrame) b, "union".equalsIgnoreCase(unionType));
		return a;
	}

	@Override
	public void setColMapping(Map<String, String> colMappings) {
		if (colMappings != null && !colMappings.isEmpty()) {
			for (Map.Entry<String, String> entry : colMappings.entrySet()) {
				if (!entry.getKey().equals(entry.getValue())) {
					throw new IllegalArgumentException(
							"Polars union requires aligned columns; rename columns before union");
				}
			}
		}
	}
}
