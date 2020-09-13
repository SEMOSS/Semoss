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

public class FormsHWDataProcessor extends FormsTableDataProcessor {
	public static final Logger LOGGER = LogManager.getLogger(FormsHWDataProcessor.class.getName());	
	
	//Constants for Columns;
	
	public int HARDWARE_COL_NUM = 2;
	public int PRODUCT_TYPE_COL_NUM = 3;
	public int QUANTITY_COL_NUM = 4;
	public int MODEL_COL_NUM = 5;
	public int TOTAL_COST_COL_NUM = 8;
	
	public FormsHWDataProcessor(){
		SYSTEM_NAME_COL_NUM = 0;
		MY_SHEET_NAME = "System_Hardware";
	}
	
	public int getColumnNumber(String name){
		switch (name)
	    {
	      case "TotalCost":
	    	  return TOTAL_COST_COL_NUM;
	      case "Model":
	    	  return MODEL_COL_NUM;
	      case "ProductType":
	    	  return PRODUCT_TYPE_COL_NUM;
	      case "Quantity":
	    	  return QUANTITY_COL_NUM;
	      case "Hardware":
	    	  return HARDWARE_COL_NUM;
	      default:
	          return -1;
	    }
	}
	
}
