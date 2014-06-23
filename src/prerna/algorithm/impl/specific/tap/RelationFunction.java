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
package prerna.algorithm.impl.specific.tap;



import java.util.ArrayList;

import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.ui.components.specific.tap.RelationPlaySheet;

/**
 * This class is used to process through two variables to identify relationships.
 */
public class RelationFunction implements IAlgorithm {
	
	Logger logger = Logger.getLogger(getClass());
	RelationPlaySheet playSheet;
	IEngine engine;
	
	ArrayList<String> rowNames = new ArrayList<String>();
	ArrayList<String> colNames = new ArrayList<String>();

	public void processRelations()
	{
		ArrayList<Object[]> processedList = new ArrayList<Object[]>();
		
		String queryString = "SELECT DISTINCT ?DataObj ?System WHERE { {?System ?Provide ?DataObj} {?System<http://www.w3.org/1999/02/22-rdf-syntax-ns#type><http://semoss.org/ontologies/Concept/ActiveSystem>} {?Provide<http://www.w3.org/2000/01/rdf-schema#subPropertyOf><http://semoss.org/ontologies/Relation/Provide>;} {?DataObj<http://www.w3.org/1999/02/22-rdf-syntax-ns#type><http://semoss.org/ontologies/Concept/DataObject>} {?Provide <http://semoss.org/ontologies/Relation/Contains/CRM> ?CRM} } BINDINGS ?CRM {('C')}";

		logger.info("PROCESSING QUERY: " + queryString);
		
		//executes the query on a specified engine
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		sjsw.setEngine(engine);
		sjsw.setQuery(queryString);
		sjsw.executeQuery();
	}

	/**
	 * Casts a given playsheet as a relation playsheet.
	 * @param playSheet 	Playsheet to be cast.
	 */

	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (RelationPlaySheet) playSheet;
	}

	@Override
	public String[] getVariables() {
		return null;
	}

	@Override
	public void execute() {
		
	}

	@Override
	public String getAlgoName() {
		return null;
	}
	
	public void setRDFEngine(IEngine engine) {
		this.engine = engine;	
	}
	
	public void setSelectedSystems(IEngine engine) {
		this.engine = engine;	
	}

}
