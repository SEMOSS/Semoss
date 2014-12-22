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
