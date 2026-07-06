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
package prerna.reactor.frame.filtermodel2;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ClearFilterModelStateCacheReactor extends AbstractFilterReactor {

	private static final Logger classLogger = LogManager.getLogger(ClearFilterModelStateCacheReactor.class);

	public ClearFilterModelStateCacheReactor() {
		this.keysToGet = new String[] {};
	}

	@Override
	public NounMetadata execute() {
		Map<String, ITableDataFrame> filterCaches = insight.getCachedFilterModelFrame();
		for (String key : filterCaches.keySet()) {
			try {
				filterCaches.remove(key).close();
			} catch (Exception e) {
				classLogger.error("Failed to clear cached filter model frame for key {}.", key, e);
			}
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
