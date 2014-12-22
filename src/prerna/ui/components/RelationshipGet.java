/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
