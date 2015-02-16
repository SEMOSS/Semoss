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
package prerna.util;

import aurelienribon.ui.css.Style;
import aurelienribon.ui.css.StyleException;

/**
 * This class is used to apply specific CSS functionality in the playsheets and UI.
 */
public class CSSApplication {
	/**
	 * Unregisters a target from the engine, assigns CSS classnames to the target, and applies a specified stylesheet to the target.
	 * @param object Object		Target for CSS to be applied to.
	 * @param cssLine String	Line of CSS code that is applied to target.
	 */
	public CSSApplication(Object object, String cssLine)
	{
	try {
		Style.unregisterTargetClassName(object);
		Style.registerTargetClassName(object, cssLine);
		Style.apply(object, new Style(getClass().getResource("styles.css")));
	} catch (StyleException e1) {
		e1.printStackTrace();
		}
	}
	
	/**
	 * Applies a specified CSS stylesheet to the target.
	 * @param object Object		Target for CSS to be applied to.
	 */
	public CSSApplication(Object object)
	{
	try {
		Style.apply(object, new Style(getClass().getResource("styles.css")));
	} catch (StyleException e1) {
		e1.printStackTrace();
		}
	}
}
