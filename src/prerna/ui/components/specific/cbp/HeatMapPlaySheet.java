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
package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This class is used to create the playsheet for a heat map.
 */
public class HeatMapPlaySheet extends BrowserPlaySheet{
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 * Runs methods from parent browser playsheet, sets dimensions for heat map playsheet, gets the user directory, and sets the file name.
	 */
	public HeatMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/heatmap.html";
	}

	/**
	 * Processes the data from a SPARQL query into an appropriate format for the specified playsheet.
	
	 * @return 	Hashtable with data from the SPARQL query results in correct format. */
	@Override
	public Hashtable processQueryData()
	{
		Hashtable dataHash = new Hashtable();
		Hashtable dataSeries = new Hashtable();
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);

			if (!((String)listElement[3]).contains("blank") && !((String)listElement[1]).contains("blank") 
					&& !((String)listElement[1]).contains("#N-A")&& !((String)listElement[1]).contains("#VALUE!") )
			{
				String methodName = (String) listElement[1];
				String groupName = (String) listElement[2];
				String key = methodName +"-"+groupName;
				if(dataHash.get(key)==null)
				{
					elementHash.put("Method", methodName);
					elementHash.put("Group", groupName);
					elementHash.put("value", 1.0);
					dataHash.put(key, elementHash);
				}
				else
				{
					elementHash = (Hashtable) dataHash.get(key);
					double count = (Double) elementHash.get("value");
					elementHash.put("value", count+1);
				}
			}
		}
		return dataHash;
	}

}
