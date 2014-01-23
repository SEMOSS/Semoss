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
 * Used to compare two numbers.
 */
public class StringNumericComparator implements Comparator<String>{

	/**
	 * Compares two given arguments for order.
	 * @param str1 String		First argument to be compared.
	 * @param str2 String		Second argument to be compared.
	
	 * @return int				Returns a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater than the second.
	 */
	@Override
	public int compare(String str1, String str2) {

		// extract numeric portion out of the string and convert them to int
		// and compare them, roughly something like this
		int num1=0;
		int num2=0;
		if(str1.indexOf(".")>0 & str2.indexOf(".")>0){
			try{
				num1 = Integer.parseInt(str1.substring(0, str1.indexOf(".")));
				num2 = Integer.parseInt(str2.substring(0, str2.indexOf(".")));
				if(num1 != num2) return num1 - num2;
			}catch(Exception e){

			}
		}
		
		return str1.compareToIgnoreCase(str2);


	}
}
