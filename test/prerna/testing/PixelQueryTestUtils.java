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
package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import prerna.reactor.export.CollectAllReactor;
import prerna.reactor.export.CollectReactor;
import prerna.reactor.frame.CreateFrameReactor;
import prerna.reactor.qs.SortReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.reactor.qs.source.FrameReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class PixelQueryTestUtils {

	// query methods

	public static PixelChain database(String engine) {
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		return db;
	}

	public static PixelChain frame(String frameName) {
		PixelChain frame = new PixelChain(FrameReactor.class, ReactorKeysEnum.FRAME.getKey(), frameName);
		return frame;
	}
	
	public static PixelChain distinct(boolean isDistinct) {
		PixelChain distinct = new PixelChain("Distinct("+isDistinct+")");
		return distinct;
	}

	public static PixelChain select(String[] cols) {
		return select(cols, null);
	}

	public static PixelChain select(String[] cols, String[] alias) {
		StringBuilder sb = new StringBuilder("Select(" + StringUtils.join(cols, ", ") + ")");
		if (alias != null) {
			// cols and alias must be the same length
			if (cols.length != alias.length) {
				fail();
			}
			sb.append(".as([" + StringUtils.join(alias, ", ") + "])");
		}
		PixelChain select = new PixelChain(sb.toString());
		return select;
	}
	
	public static PixelChain filter(String column, String comparison, Object val) {
		PixelChain select = new PixelChain("Filter"+buildFilter(column, comparison, val));
		return select;
	}

	public static String buildFilter(String column, String comparison, Object val) {
		String result = "";
		result += "(" + column + " " + comparison + "\"" + val + "\")";
		return result;
	}
	
	public static PixelChain groupBy(String[] cols) {
		PixelChain group = new PixelChain("Group(" + StringUtils.join(cols, ", ") + ")");
		return group;

	}
	public static PixelChain sort(String[] cols, String sortDir) {
		PixelChain sort = new PixelChain(SortReactor.class, ReactorKeysEnum.COLUMNS.getKey(), cols,
				ReactorKeysEnum.SORT.getKey(), sortDir);
		return sort;
	}


	public static PixelChain collect(int numRows) {
		PixelChain pc = new PixelChain(CollectReactor.class, ReactorKeysEnum.LIMIT.getKey(), numRows);
		return pc;
	}

	public static PixelChain collectAll() {
		PixelChain pc = new PixelChain(CollectAllReactor.class);
		return pc;
	}
	
	// import methods
	
	public static String createFramePixel(String frameType, String frameAlias, boolean override) {
		String framePixel = ApiSemossTestUtils.buildPixelCall(CreateFrameReactor.class,
				ReactorKeysEnum.FRAME_TYPE.getKey(), frameType, "override", override, ReactorKeysEnum.ALIAS.getKey(),
				frameAlias);
		return framePixel;
	}
	
	public static PixelChain importPixel(String framePixel) {
		framePixel = framePixel.replace(";", "");
		PixelChain importPixel = new PixelChain("Import(frame=[" + framePixel + "])");
		return importPixel;
	}
	
	// query results

	public static List<Map<String, Object>> flushTaskToList(BasicIteratorTask task, Integer rowCount) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		while (task.hasNext()) {
			list.add(task.next().flushRowToMap());
		}
		assertEquals(rowCount, list.size());
		try {
			task.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return list;
	}

	public static PixelChain limit(int i) {
		PixelChain limit = new PixelChain("Limit("+i+")");
		return limit;
	}
	
	public static PixelChain offset(int i) {
		PixelChain offset = new PixelChain("Offset("+i+")");
		return offset;
	}

}
