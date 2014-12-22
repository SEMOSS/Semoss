/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.om;

import java.util.Hashtable;

// a class that keeps the output
// typically it keeps the data
// the insight that was used to run it


public class Output {

	// the insight that is being utilized
	Insight insight = null;
	
	// modified SPARQL
	
	
	// the data for the run
	Object data = null;
	
	// parameters utilized for this output
	Hashtable paramNames = new Hashtable();
	
	// add action
	// following are the different types of actions that can be performed
	// extending 
	// overlaying 
	// I will not worry about this right now
	
	
}
