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
package prerna.util;

import java.util.Comparator;


/**
 * Used to compare two properties.
 */
public class PropComparator implements Comparator<String>{

	/**
	 * Compares two properties.
	 * Checks whether they are equal to the name of a node, type of node, name of an edge, type of an edge, or URI based on the constants class.
	 * Returns -1 if first string equals one of the listed constants and 1 if the second string equals one of the listed constants.
	 * @param str1 String		First property to be compared.
	 * @param str2 String		Second property to be compared.
	
	 * @return int 				Returns -1, 0, or 1 */
	@Override
	public int compare(String str1, String str2) {
		if(str1.equals(Constants.VERTEX_NAME)) return -1;
		else if(str2.equals(Constants.VERTEX_NAME)) return 1;
		else if(str1.equals(Constants.VERTEX_TYPE)) return -1;
		else if(str2.equals(Constants.VERTEX_TYPE)) return 1;
		else if(str1.equals(Constants.EDGE_NAME)) return -1;
		else if(str2.equals(Constants.EDGE_NAME)) return 1;
		else if(str1.equals(Constants.EDGE_TYPE)) return -1;
		else if(str2.equals(Constants.EDGE_TYPE)) return 1;
		else if(str1.equals(Constants.URI)) return -1;
		else if(str2.equals(Constants.URI)) return 1;

		return str1.compareToIgnoreCase(str2);


	}
}
