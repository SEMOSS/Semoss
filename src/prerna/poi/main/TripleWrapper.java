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
package prerna.poi.main;

public class TripleWrapper {

	//core of the triple
	private String obj1;
	private String pred;
	private String obj2;
	
	//core plus extremities
	private String obj1Expanded = "NA";
	private String predExpanded = "NA";
	private String obj2Expanded = "NA";

	private String docName;
	private String sentence;
	
	private int obj1Count;
	private int predCount;
	private int obj2Count;

	public TripleWrapper() {

	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append(obj1);
		result.append(">");
		result.append(pred);
		result.append(">");
		result.append(obj2);
		result.append(";\n");
		result.append(obj1Expanded);
		result.append(">");
		result.append(predExpanded);
		result.append(">");
		result.append(obj2Expanded);
		result.append("\n");

		return result.toString();
	}

	public String getObj1() {
		return obj1;
	}

	public void setObj1(String obj1) {
		this.obj1 = obj1;
	}

	public String getPred() {
		return pred;
	}

	public void setPred(String pred) {
		this.pred = pred;
	}

	public String getObj2() {
		return obj2;
	}

	public void setObj2(String obj2) {
		this.obj2 = obj2;
	}

	public String getObj1Expanded() {
		return obj1Expanded;
	}

	public void setObj1Expanded(String obj1Expanded) {
		this.obj1Expanded = obj1Expanded;
	}

	public String getPredExpanded() {
		return predExpanded;
	}

	public void setPredExpanded(String predExpanded) {
		this.predExpanded = predExpanded;
	}

	public String getObj2Expanded() {
		return obj2Expanded;
	}

	public void setObj2Expanded(String obj2Expanded) {
		this.obj2Expanded = obj2Expanded;
	}

	public String getDocName() {
		return docName;
	}

	public void setDocName(String docName) {
		this.docName = docName;
	}

	public String getSentence() {
		return sentence;
	}

	public void setSentence(String sentence) {
		this.sentence = sentence;
	}

	public int getObj1Count() {
		return obj1Count;
	}

	public void setObj1Count(int obj1Count) {
		this.obj1Count = obj1Count;
	}

	public int getPredCount() {
		return predCount;
	}

	public void setPredCount(int predCount) {
		this.predCount = predCount;
	}

	public int getObj2Count() {
		return obj2Count;
	}

	public void setObj2Count(int obj2Count) {
		this.obj2Count = obj2Count;
	}

}
