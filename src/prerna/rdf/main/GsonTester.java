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
package prerna.rdf.main;

import java.util.Hashtable;

import com.google.gson.Gson;

/**
 */
public class GsonTester {
	/**
	 * Method main.
	 * @param args String[]
	 */
	public static void main(String [] args)
	{
		Hashtable hash = new Hashtable();
		hash.put("1", "arg1");
		hash.put("2", "arg1");
		hash.put("3", "arg1");
		hash.put("4", "arg1");
		Hashtable hash2 = new Hashtable();
		hash2.put("1", "arg1");
		hash2.put("2", "arg1");
		hash2.put("3", "arg1");
		hash2.put("4", "arg1");
		hash.put("Hash", hash2);
		
		
		Gson gson = new Gson();
		System.out.println(gson.toJson(hash));
		
	}

}
