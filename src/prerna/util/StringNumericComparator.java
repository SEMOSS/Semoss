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
			}catch(RuntimeException e){
				System.out.println("ignored");
			}
		}
		
		return str1.compareToIgnoreCase(str2);


	}
}
