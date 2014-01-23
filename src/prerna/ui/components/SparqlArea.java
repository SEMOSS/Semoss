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
package prerna.ui.components;

import javax.swing.JTextArea;

/**
 * This class creates the area where a user can input a custom SPARQL query.
 */
public class SparqlArea extends JTextArea {
	
	String questionName = null;
	String keyName = null;
	
	/**
	 * Sets the question name.
	 * @param questionName String
	 */
	public void setQuestionName(String questionName)
	{
		this.questionName = questionName;
	}
	
	/**
	 * Gets the question name.
	
	 * @return String */
	public String getQuestionName()
	{
		return questionName;
	}
}
