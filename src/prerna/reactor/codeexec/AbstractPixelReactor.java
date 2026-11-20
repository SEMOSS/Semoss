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
package prerna.reactor.codeexec;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractPixelReactor extends AbstractReactor {

	// the code that was executed
	protected String code = null;

	public AbstractPixelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CODE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String decodedCode = getDecodedCode();
		if (decodedCode == null || decodedCode.trim().isEmpty()) {
			throw new IllegalArgumentException("Must provide pixel code to execute.");
		}

		this.code = fillVars(decodedCode);
		PixelRunner pixelReturn = this.insight.runPixel(this.code);
		Map<String, Object> runnerWrapper = new HashMap<String, Object>();
		runnerWrapper.put("runner", pixelReturn);
		return new NounMetadata(runnerWrapper, PixelDataType.PIXEL_RUNNER, PixelOperationType.SUB_SCRIPT);
	}

	public String getExecutedCode() {
		return this.code;
	}

	@Override
	public String getReactorDescription() {
		return "Run Pixel code in the current insight";
	}

	/**
	 * Decode the code string from the pixel
	 *
	 * @return The decoded code string
	 */
	protected abstract String getDecodedCode();
}
