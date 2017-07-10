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
package prerna.ui.helpers;

import java.util.Arrays;
import java.util.List;

import prerna.om.OldInsight;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.ISEMOSSTransformation;

/**
 * This class helps with running the undo view method for a playsheet.
 */
public class PlaysheetUndoRunner implements Runnable{

	OldInsight insight = null;
	
	/**
	 * Constructor for PlaysheetUndoRunner.
	 * @param playSheet GraphPlaySheet
	 */
	public PlaysheetUndoRunner(OldInsight insight)
	{
		this.insight = insight;
	}
	
	/**
	 * Method run. Calls the undo view method on the local play sheet.
	 */
	@Override
	public void run() {
		DataMakerComponent lastComp = insight.getDataMakerComponents().get(insight.getNumComponents()-1);
		String id = null;
		List<ISEMOSSTransformation> postTrans = lastComp.getPostTrans();
		if(postTrans!=null && !postTrans.isEmpty()){
			ISEMOSSTransformation lastTrans = postTrans.get(postTrans.size()-1);
			id = lastTrans.getId();
		}
		if(id == null){
			id = lastComp.getPreTrans().get(lastComp.getPreTrans().size() - 1).getId();
		}
		insight.undoProcesses(Arrays.asList(new String[]{id}));
		((GraphPlaySheet)insight.getPlaySheet()).undoView();
	}
}
