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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JList;

import prerna.poi.main.POIWriter;
import prerna.poi.main.RelationshipLoadingSheetWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class is used to generate items in the GL and is used to update the cost database.
 */
public class GLItemGeneratorSelfReportedFutureInterfaces {

	private IEngine hrCore;
	private IEngine futureState;
	private IEngine futureCostState;
	private IEngine tapCost;
	
	/**
	 * Constructor for GLItemGeneratorICDValidated.
	 */
	public GLItemGeneratorSelfReportedFutureInterfaces(IEngine hrCore, IEngine futureState, IEngine futureCostDB, IEngine costDB) {
		this.hrCore = hrCore;
		this.futureState = futureState;
		this.futureCostState = futureCostDB;
		this.tapCost = costDB;
	}

	public void genData(){
//		genGLItems();
//		fillNecessaryHashes();
//		setHashesInGenerator();
//		runGenerator();
//		getData();
//		insertData();
	}
}
