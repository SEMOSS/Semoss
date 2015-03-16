/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.nameserver;

public final class MasterDatabaseConstants {
	
	private MasterDatabaseConstants() {
		
	}
	
	// similarity cutoff value
	public static final double SIMILARITY_CUTOFF = 0.20;
	public static final double MAIN_NOUN_WEIGHT = 0.8;
	public static final double OTHER_NOUN_WEIGHT = 0.2;
	
	// keys for passing insights
	public static final String DB_KEY = "database";
	public static final String SCORE_KEY = "similarityScore";
	public static final String QUESITON_KEY = "question";
	public static final String TYPE_KEY = "type";
	public static final String PERSPECTIVE_KEY = "perspective";
	public static final String INSTANCE_KEY = "instances";
	public static final String VIZ_TYPE_KEY = "viz";
	public static final String ENGINE_URI_KEY = "engineURI";
}
