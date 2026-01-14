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
package prerna.junit.pixel;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

import prerna.sablecc2.PixelRunner;

public class PixelUnitConsole extends PixelUnit {

	@Test
	public void runConsole() {
		String end = "";
		while(!end.equalsIgnoreCase("end")) {
			try {
				
				// Initialize the "test" - setup state
				initializeTest(false);

				// Read pixel from tester
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
								
				classLogger.info("Enter Pixel command (separated by ; and on one line): ");
				String pixel = reader.readLine();   
				pixel = pixel.trim();
				
				// Run the pixel
				if(!pixel.isEmpty()) {
					PixelRunner returnData = runPixel(pixel);
					JsonArray allPixelReturns = getPixelReturns(returnData);
					JsonElement lastPixelReturn = allPixelReturns.get(allPixelReturns.size() - 1);
					
					classLogger.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
					classLogger.info("ALL: ");
					System.out.println(GSON_PRETTY.toJson(allPixelReturns));
					classLogger.info("LAST: ");
					System.out.println(GSON_PRETTY.toJson(lastPixelReturn));
					classLogger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
				} else {
					
					// Otherwise terminate if the tester enters without typing a Pixel
					end = "end";
				}
			} catch(Exception e) {
				classLogger.error("Error: ", e);
			} finally {
				
				// Destroy the "test" - reset state
				destroyTest();
			}
		}
	}
	
}