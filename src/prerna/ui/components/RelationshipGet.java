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

import java.util.Hashtable;
import java.util.Vector;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Given a type, this class is used to get relationships and questions.
 */
public class RelationshipGet {
	
	
	/**
	 * Gets relationships for a certain type.
	 * @param type 	Specified type.
	
	 * @return Vector	Vector that contains new questions. */
	public static Vector getRelationship(String type)
	{
		Vector questionV = new Vector();
		Hashtable extendHash = (Hashtable)DIHelper.getInstance().getLocalProp(Constants.EXTEND_TABLE);
		questionV = (Vector) extendHash.get(type);
		Vector newQuestionV = new Vector();
		int vSize = questionV.size();
		for (int i=0;i<vSize;i++)
		{
			String qString = (String) questionV.get(i);
			String[] qStringArray = qString.split(";");
    		String newQString = qStringArray[1];
    		newQuestionV.addElement(newQString);
		}
		return newQuestionV;
		
	}
	
}
