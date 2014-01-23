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
package prerna.ui.helpers;

import prerna.algorithm.api.IAlgorithm;

/**
 * This class helps with running algorithms.
 */
public class AlgorithmRunner implements Runnable{

	IAlgorithm algo = null;
	
	/**
	 * Constructor for AlgorithmRunner.
	 * @param algo IAlgorithm - the algorithm that this is set to.
	 */
	public AlgorithmRunner(IAlgorithm algo)
	{
		this.algo = algo;
	}

	/**
	 * Method run. Calls the execute method and runs the algorithm.
	 */
	@Override
	public void run() {
		algo.execute();
	}
	
}
