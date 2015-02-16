/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.ui.swing.custom;

import aurelienribon.ui.components.AruiFunctions;
import aurelienribon.ui.components.AruiRules;
import aurelienribon.ui.components.Button;
import aurelienribon.ui.components.TabPanel;
import aurelienribon.ui.css.Style;

/**
 * Class that imports additional CSS functionality.
 */
public class CustomAruiStyle {
	/**
	 * Executes imported CSS.
	 */
	public static void init() {
		Style.registerRule(AruiRules.FOREGROUND_MOUSEOVER);
		Style.registerRule(AruiRules.FOREGROUND_MOUSEDOWN);
		Style.registerRule(AruiRules.FOREGROUND_SELECTED);
		Style.registerRule(AruiRules.FOREGROUND_UNSELECTED);
		Style.registerRule(AruiRules.STROKE);
		Style.registerRule(AruiRules.STROKE_MOUSEOVER);
		Style.registerRule(AruiRules.STROKE_MOUSEDOWN);
		Style.registerRule(AruiRules.STROKE_SELECTED);
		Style.registerRule(AruiRules.STROKE_UNSELECTED);
		Style.registerRule(AruiRules.FILL);
		Style.registerRule(AruiRules.FILL_MOUSEOVER);
		Style.registerRule(AruiRules.FILL_MOUSEDOWN);
		Style.registerRule(AruiRules.FILL_SELECTED);
		Style.registerRule(AruiRules.FILL_UNSELECTED);
		Style.registerRule(AruiRules.CORNERRADIUS);

		Style.registerFunction(AruiFunctions.GROUPBORDER);

		Style.registerProcessor(Button.PROCESSOR);
		Style.registerProcessor(TabPanel.PROCESSOR);
		Style.registerProcessor(ToggleButton.PROCESSOR);
		Style.registerProcessor(CustomButton.PROCESSOR);
	}
}
