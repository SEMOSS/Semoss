/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.algorithm.impl.DataLatencyPerformer;
import prerna.algorithm.impl.IslandIdentifierProcessor;
import prerna.algorithm.impl.LoopIdentifierProcessor;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;

public class InterfaceGraphPlaySheet extends GraphPlaySheet {	

	public Map runLoopIdentifer(Map webDataHash) {
		Map retHash = new Hashtable();
        LoopIdentifierProcessor pro = new LoopIdentifierProcessor();
		pro.setGraphData(this.getVerts(), this.getEdges());
		pro.setPlaySheet(this);	
		pro.executeWeb();
		retHash = pro.getLoopEdges();
		return retHash;
	}
	
	public Map runIslandIdentifier(Map webDataHash) {
		Gson gson = new Gson();
		Map retHash = new Hashtable();
        IslandIdentifierProcessor pro = new IslandIdentifierProcessor();
		if (!(webDataHash.get("selectedNodes") == (null))) {
			ArrayList<Hashtable<String, Object>> nodesArray = gson.fromJson(gson.toJson(webDataHash.get("selectedNodes")), new TypeToken<ArrayList<Hashtable<String, Object>>>() {}.getType());
			SEMOSSVertex[] pickedVertex = new SEMOSSVertex[1];
			Object desiredUri = nodesArray.get(0).get("uri");
			Iterator<SEMOSSVertex> myIt = this.getVerts().iterator();
			while(pickedVertex[0] == null && myIt.hasNext()){
				SEMOSSVertex vert = myIt.next();
				if(vert.uri.equals(desiredUri)){
					pickedVertex[0] = vert;
				}
			}
			pro.setSelectedNodes(pickedVertex);
		} else {
			SEMOSSVertex[] pickedVertex = new SEMOSSVertex[]{};
			pro.setSelectedNodes(pickedVertex);
		}
		pro.setGraphData(this.getVerts(), this.getEdges());			
		pro.setPlaySheet(this);	
		pro.executeWeb();
		retHash = pro.getIslandEdges();
		return retHash;
	}
	
	public Map runDataLatency(Map webDataHash) {
		Gson gson = new Gson();
		Map retHash = new Hashtable();
        SEMOSSVertex[] pickedVertex;
		if (!(webDataHash.get("selectedNodes") == (null))) {
			List<Map<String, Object>> nodesArray = gson.fromJson(gson.toJson(webDataHash.get("selectedNodes")), new TypeToken<List<Map<String, Object>>>() {}.getType());
			pickedVertex = new SEMOSSVertex[1];Object desiredUri = nodesArray.get(0).get("uri");
			Iterator<SEMOSSVertex> myIt = this.getVerts().iterator();
			while(pickedVertex[0] == null && myIt.hasNext()){
				SEMOSSVertex vert = myIt.next();
				if(vert.uri.equals(desiredUri)){
					pickedVertex[0] = vert;
				}
			}
		} else {
			pickedVertex = new SEMOSSVertex[]{};
		}
		DataLatencyPerformer performer = new DataLatencyPerformer(this, pickedVertex);
		double sliderValue = 1000;
		performer.setValue(sliderValue);			
		performer.executeWeb();			
		retHash = performer.getEdgeScores();     
		return retHash;
	}
}
