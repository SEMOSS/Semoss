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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Vector;

import org.mvel2.MVEL;

import prerna.ui.components.VertexFilterData;
import prerna.ui.components.playsheets.GraphPlaySheet;

public class CustomNodeCreator {
	
	GraphPlaySheet ps = null;
	String selected = null;
	String expression = null;
	
	
	public void setGraphPlaySheet(GraphPlaySheet ps)
	{
		this.ps = ps;
	}
	
	public void execute()
	{
		// core job of this class is to create custom nodes
		// Here is how it works
		// 1. Gets the reference to graph playsheet
		// 2. Displays the types of nodes in VertexFilterData
		// 3. The user selects one of the types of node
		// 4. Next it asks for a formula
		// 5. Now it runs through the formula for each of it
		// 6. Telling us what is the evaluated piece

		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		String input = null;
		try {
			while((input = reader.readLine()) != null)
			{
				if(input.equalsIgnoreCase("H"))
				{
					System.out.println("L - List all the types of nodes on this graph");
					System.out.println("S - Select a type of node from the list of nodes. S <Node to Select>");
					System.out.println("E - Express what you would like to do with this E <Expression>");
					System.out.println("R - Run this and output the results ");
					System.out.println("C - Clear all and start fresh ");
					System.out.println("H - This menu / help ");
				}
				if(input.equalsIgnoreCase("L"))
				{
					// displays the vertex filter data
					VertexFilterData vfd = ps.filterData;
					System.out.println("Vertex Types Available ");
					System.out.println("======================= ");
					Enumeration keys = vfd.typeHash.keys();
					int keyCount = 1;
					while(keys.hasMoreElements())
					{
						System.out.println(keyCount + " ." + keys.nextElement());
						keyCount++;
					}
				}
				if(input.toUpperCase().startsWith("S"))
				{
					StringTokenizer tokens = new StringTokenizer(input);
					tokens.nextToken();
					selected = tokens.nextToken();
				}
				if(input.toUpperCase().startsWith("E"))
				{
					StringTokenizer tokens = new StringTokenizer(input);
					tokens.nextToken();
					expression = tokens.nextToken();
				}
				if(input.equalsIgnoreCase("R"))
				{
					// show me the MVEL Magic BOY !!
					if(selected != null && ps.filterData.typeHash.containsKey(selected))
					{
						Vector objs = ps.filterData.typeHash.get(selected);
						for(int vertIndex = 0;vertIndex <= objs.size();vertIndex++)
							System.out.println("" + MVEL.eval(expression, objs.elementAt(vertIndex)));
						
						// the patterns are
						// basic properties
						// navigate to another node and then properties
						
						
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	

}
