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
package prerna.reactor.frame;

import java.util.EnumSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.ds.rdbms.postgres.PostgresFrame;
import prerna.ds.rdbms.sqlite.SQLiteFrame;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;

public final class FrameFactory {

	private static final Logger classLogger = LogManager.getLogger(FrameFactory.class);

	private static final String CLASS_NAME = FrameFactory.class.getName();
	private static final Set<RdbmsTypeEnum> GRID_TYPES = EnumSet.of(RdbmsTypeEnum.H2_DB, RdbmsTypeEnum.SQLITE,
			RdbmsTypeEnum.POSTGRES);
	// this is so we only grab from DIHelper once
	private static boolean INIT = false;
	// grab default frame
	private static String DEFAULT_FRAME_TYPE = null;
	private static RdbmsTypeEnum RDBMS_TYPE = null;

	private FrameFactory() {

	}

	/**
	 * Create a new frame of the requested type. The type is case insensitive, and
	 * DEFAULT uses the DEFAULT_FRAME_TYPE setting.
	 * <ul>
	 * <li>GRID or H2: a grid frame on the DEFAULT_GRID_TYPE database</li>
	 * <li>GRAPH or TINKER: a Tinker graph frame</li>
	 * <li>R, RFRAME or DATATABLE: an R data.table</li>
	 * <li>PYTHON, PY, PYFRAME or PANDAS: a pandas frame</li>
	 * <li>NATIVE or ENGINE: a native frame that queries its engine directly</li>
	 * </ul>
	 * Any other type gives a grid frame.
	 *
	 * @param insight   the insight that owns the frame, whose R or Python
	 *                  translator backs R and pandas frames
	 * @param frameType the frame type, or DEFAULT
	 * @param alias     the frame name
	 * @return the new frame
	 * @throws Exception if the frame cannot be created
	 */
	public static ITableDataFrame getFrame(Insight insight, String frameType, String alias) throws Exception {
		frameType = frameType.toUpperCase();

		Logger logger = null;
		if (!INIT) {
			init();
		}

		if (frameType.equals("DEFAULT")) {
			frameType = DEFAULT_FRAME_TYPE;
		}
		switch (frameType) {
		case "GRID": {
			return getGrid(alias);
		}
		case "H2": {
			return getGrid(alias);
		}

		case "GRAPH": {
			TinkerFrame frame = new TinkerFrame(alias);
			return frame;
		}
		case "TINKER": {
			TinkerFrame frame = new TinkerFrame(alias);
			return frame;
		}

		case "R": {
			logger = LogManager.getLogger(CLASS_NAME);
			return new RDataTable(insight.getRJavaTranslator(logger), alias);
		}
		case "RFRAME": {
			logger = LogManager.getLogger(CLASS_NAME);
			return new RDataTable(insight.getRJavaTranslator(logger), alias);
		}
		case "DATATABLE": {
			logger = LogManager.getLogger(CLASS_NAME);
			return new RDataTable(insight.getRJavaTranslator(logger), alias);
		}

		case "PYTHON": {
			PandasFrame frame = new PandasFrame(alias, insight.getPyTranslator());
			return frame;
		}
		case "PY": {
			PandasFrame frame = new PandasFrame(alias, insight.getPyTranslator());
			return frame;
		}
		case "PYFRAME": {
			PandasFrame frame = new PandasFrame(alias, insight.getPyTranslator());
			return frame;
		}
		case "PANDAS": {
			PandasFrame frame = new PandasFrame(alias, insight.getPyTranslator());
			return frame;
		}

		case "NATIVE": {
			NativeFrame frame = new NativeFrame(alias);
			return frame;
		}
		case "ENGINE": {
			NativeFrame frame = new NativeFrame(alias);
			return frame;
		}

		default: {
			return getGrid(alias);
		}

		}
	}

	/**
	 * Create a grid frame on the DEFAULT_GRID_TYPE database: SQLite, Postgres, or
	 * H2 for anything else.
	 *
	 * @param alias the frame name
	 * @return the new grid frame
	 * @throws Exception if the frame cannot be created
	 */
	private static ITableDataFrame getGrid(String alias) throws Exception {
		if (RDBMS_TYPE == RdbmsTypeEnum.SQLITE) {
			return new SQLiteFrame(alias);
		} else if (RDBMS_TYPE == RdbmsTypeEnum.POSTGRES) {
			return new PostgresFrame(alias);
		}

		return new H2Frame(alias);
	}

	/**
	 * Read the DEFAULT_FRAME_TYPE and DEFAULT_GRID_TYPE settings once, on the first
	 * frame request. A missing DEFAULT_FRAME_TYPE is GRID.
	 */
	private static void init() {
		DEFAULT_FRAME_TYPE = Utility.getDIHelperProperty(Constants.DEFAULT_FRAME_TYPE);
		RDBMS_TYPE = getGridType(Utility.getDIHelperProperty(Constants.DEFAULT_GRID_TYPE));

		if (DEFAULT_FRAME_TYPE == null) {
			DEFAULT_FRAME_TYPE = "GRID";
		}
		INIT = true;
	}

	/**
	 * Resolve the configured default grid type to a database that has a grid frame.
	 * A missing value is H2, and an unknown value or a database without a grid
	 * frame is logged and also falls back to H2.
	 *
	 * @param configuredType the DEFAULT_GRID_TYPE value
	 * @return H2_DB, SQLITE or POSTGRES
	 */
	static RdbmsTypeEnum getGridType(String configuredType) {
		if (configuredType == null || configuredType.trim().isEmpty()) {
			return RdbmsTypeEnum.H2_DB;
		}
		RdbmsTypeEnum gridType = RdbmsTypeEnum.getEnumFromString(configuredType.trim());
		if (GRID_TYPES.contains(gridType)) {
			return gridType;
		}
		classLogger.error("Default grid type '{}' is not one of {}. Defaulting to H2_DB", configuredType, GRID_TYPES);
		return RdbmsTypeEnum.H2_DB;
	}

	/**
	 * Check whether query results on a frame can be cached. R and pandas frames
	 * can, and a native frame can when its engine type is cacheable. Grid and graph
	 * frames cannot.
	 *
	 * @param frame the frame, which may be null
	 * @return true when query results on the frame can be cached, false for a null
	 *         frame
	 */
	public static boolean canCacheFrameQueries(ITableDataFrame frame) {
		if (frame == null) {
			return false;
		}
		boolean cache = false;
		if (frame instanceof RDataTable) {
			cache = true;
		} else if (frame instanceof PandasFrame) {
			cache = true;
		} else if (frame instanceof NativeFrame) {
			cache = ((NativeFrame) frame).engineQueryCacheable();
		}

		return cache;
	}
}
