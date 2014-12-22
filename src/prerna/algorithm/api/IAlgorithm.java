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
package prerna.algorithm.api;

import prerna.ui.components.api.IPlaySheet;


/**
 * This interface is used to standardize the functionality of the algorithms applied to various play sheets.  Algorithms are, 
 * in general terms, a way of using the data and attributes of a play sheet to gather insight that is not immediately apparent 
 * from the play sheet by itself.
 *
 * @author karverma
 * @version $Revision: 1.0 $
 */
public interface IAlgorithm {
	
	/**
	 * Sets the play sheet that the algorithm is to be associated with and run on.
	 * @param playSheet the play sheet that the algorithm is to be run on
	 */
	public void setPlaySheet(IPlaySheet playSheet);
	
	/**
	 * Gets a String Array of variables that need to be fed into the algorithm in order for the algorithm to be successfully run.
	
	 * @return names of variables that the algorithm requires */
	public String[] getVariables();
	

	/**
	 * Runs the algorithm.
	
	 */
	public void execute();


	/**
	 * Gets the name of the algorithm in String form.
	
	 * @return String name of the algorithm */
	public String getAlgoName();
	
}
