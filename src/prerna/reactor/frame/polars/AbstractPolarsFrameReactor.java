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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 or GPLv2, as applicable.
 *******************************************************************************/
package prerna.reactor.frame.polars;

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.ICodeExecution;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PolarsFrame;
import prerna.om.Variable.LANGUAGE;
import prerna.reactor.frame.AbstractFrameReactor;

public abstract class AbstractPolarsFrameReactor extends AbstractFrameReactor implements ICodeExecution {

	private final List<String> codeExecuted = new ArrayList<>();

	protected PolarsFrame getPolarsFrame() {
		if (!(getFrame() instanceof PolarsFrame)) {
			throw new IllegalArgumentException("This reactor requires a Polars frame");
		}
		return (PolarsFrame) getFrame();
	}

	protected Object parseValue(PolarsFrame frame, String column, String rawValue) {
		if (rawValue == null || rawValue.equalsIgnoreCase("null") || rawValue.equalsIgnoreCase("na")
				|| rawValue.equalsIgnoreCase("nan")) {
			return null;
		}
		String uniqueName = frame.getName() + "__" + cleanColumn(column);
		SemossDataType type = frame.getMetaData().getHeaderTypeAsEnum(uniqueName);
		if (type == SemossDataType.INT) {
			return Long.valueOf(rawValue);
		}
		if (type == SemossDataType.DOUBLE) {
			return Double.valueOf(rawValue);
		}
		if (type == SemossDataType.BOOLEAN) {
			return Boolean.valueOf(rawValue);
		}
		return rawValue;
	}

	protected String cleanColumn(String column) {
		if (column != null && column.contains("__")) {
			return column.split("__", 2)[1];
		}
		return column;
	}

	protected void addExecutedCode(String code) {
		this.codeExecuted.add(code);
	}

	@Override
	public String getExecutedCode() {
		return String.join("\n", this.codeExecuted);
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.PYTHON;
	}

	@Override
	public boolean isUserScript() {
		return false;
	}
}
