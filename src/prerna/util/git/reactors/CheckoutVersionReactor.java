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
package prerna.util.git.reactors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.git.GitRepoUtils;


public class CheckoutVersionReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CheckoutVersionReactor.class);



	// checks out this app to a specific version
	// this is a bit dangerous if another user is operating on this at the same time
	// I dont know if we should use the cache copy and move there
	// if the version is not provided it will reset checkout
	
	public CheckoutVersionReactor() {
		this.keysToGet = new String[]{"version"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String assetFolder = this.insight.getInsightFolder(); // we need it where this would be the cache
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		
		// I need to do the job of creating this directory i.e. the name of the repo
		// TBD
		
		String version = null;
		
		if(keyValue.containsKey(keysToGet[0]))
			version = keyValue.get(keysToGet[0]);
		
		// I need a better way than output
		// probably write the file and volley the file ?
		String output = null;
		try {
			if(version != null)
			{
				GitRepoUtils.checkout(assetFolder, version); 
				output = "Version - " + version + " active now";
			}
			else
			{
				GitRepoUtils.resetCheckout(assetFolder);
				output = "Version - latest" + " active now";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
