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
package prerna.ui.components.specific.tap.forms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FormsSWDataProcessor extends FormsTableDataProcessor {
	public static final Logger LOGGER = LogManager.getLogger(FormsSWDataProcessor.class.getName());	
	
	//Constants for Columns;
	
	public int SOFTWARE_COL_NUM = 2;
	public int VERSION_COL_NUM = 6;
	public int NUM_OF_LICENSES_COL_NUM = 3;
	public int TOTAL_COST_COL_NUM = 8;
	
	public FormsSWDataProcessor(){
		SYSTEM_NAME_COL_NUM = 0;
		MY_SHEET_NAME = "System_Software";
	}
	
	public int getColumnNumber(String name){
		switch (name)
	    {
	      case "TotalCost":
	    	  return TOTAL_COST_COL_NUM;
	      case "Version":
	    	  return VERSION_COL_NUM;
	      case "Quantity":
	    	  return NUM_OF_LICENSES_COL_NUM;
	      case "Software":
	    	  return SOFTWARE_COL_NUM;
	      default:
	          return -1;
	    }
	}
	
}
