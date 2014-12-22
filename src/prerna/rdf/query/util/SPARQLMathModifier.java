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
package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Hashtable;

public class SPARQLMathModifier extends SPARQLAbstractReturnModifier {

	public final static SPARQLModifierConstant ADD = new SPARQLModifierConstant("+");
	public final static SPARQLModifierConstant SUBTRACT = new SPARQLModifierConstant("-");
	public final static SPARQLModifierConstant MULTIPLY = new SPARQLModifierConstant("*");
	public final static SPARQLModifierConstant DIVIDE = new SPARQLModifierConstant("/");
	ArrayList dataList;
	ArrayList<SPARQLModifierConstant> opList;

	public void createModifier(ArrayList dataList, ArrayList<SPARQLModifierConstant> opList) {
		this.dataList = dataList;
		this.opList = opList;
		for (Object ls:dataList)
		{
			if (!(ls instanceof ISPARQLReturnModifier) && !(ls instanceof TriplePart) && !(ls instanceof Double) && !(ls instanceof Integer))
			{
				throw new IllegalArgumentException("Only integers, doubles, tripleparts, and SPARQLReturnModifiers can be part of a SPARQLMathModifier");
			}
			else if (!(dataList.size()-1==opList.size()))
			{
				throw new IllegalArgumentException("Number of variables has to be ONE more than number of operators, think \"x * y\"");
			}
		}

	}
	
	@Override
	public String getModifierAsString() {
		
		String modString = "";
		for (int enIdx = 0; enIdx<dataList.size(); enIdx++)
		{
			if(dataList.get(enIdx) instanceof ISPARQLReturnModifier)
			{
				//don't need to fill really use the ID here because there is only one modifier
				ISPARQLReturnModifier mod = (ISPARQLReturnModifier)dataList.get(enIdx);
				modString = modString + "("+mod.getModifierAsString()+")";
			}
			else if (dataList.get(enIdx) instanceof TriplePart)
			{
				TriplePart part= (TriplePart)dataList.get(enIdx);
				modString = modString+SPARQLQueryHelper.createComponentString(part);
			}
			else if (dataList.get(enIdx) instanceof Double || dataList.get(enIdx) instanceof Integer)
			{
				modString = modString + dataList.get(enIdx)+"";
			}
			//add operator if it's not last element
			if(enIdx <dataList.size()-1)
			{
				modString = modString + " " + opList.get(enIdx).getConstant() + " ";
			}
		}
		return modString;
	}
}
