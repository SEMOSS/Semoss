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

import prerna.om.OldInsight;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;

/**
 * This class helps with running the overlay view method for a playsheet.
 */
public class InsightOverlayRunner implements Runnable{

	OldInsight insight = null;
	DataMakerComponent[] dmComponents;
	
	
	/**
	 * Constructor for PlaysheetOverlayRunner.
	 * @param playSheet IPlaySheet
	 */
	public InsightOverlayRunner(OldInsight insight, DataMakerComponent[] dmComponents)
	{
		this.insight = insight;
		this.dmComponents = dmComponents;
	}

	
	/**
	 * Method run. Calls the overlay view method on the local play sheet.
	 */
	@Override
	public void run() {
//		if(playSheet instanceof AbstractPlaySheet)
//			((AbstractPlaySheet)playSheet).setAppend(true);
//		playSheet.createData();
//		playSheet.runAnalytics();
//		playSheet.overlayView();
		
//		IDataMaker dm = playSheet.getDataMaker();
		((GraphPlaySheet)insight.getPlaySheet()).updateProgressBar("Processing new data maker component", 25);
		for(DataMakerComponent dmc : this.dmComponents){
			insight.processDataMakerComponent(dmc);
		}
		insight.getPlaySheet().runAnalytics();
		insight.getPlaySheet().overlayView();
	}

}