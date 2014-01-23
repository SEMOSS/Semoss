/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
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
