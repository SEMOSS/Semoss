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

import java.util.Hashtable;

public class SPARQLCustomModifier implements ISPARQLReturnModifier{
	
	String modString;

	public void setModString(String modString) {
		this.modString = modString;
	}

	@Override
	public String getModifierAsString() {
		return modString;
	}
	
	@Override
	public void setLowerLevelModifier(
			Hashtable<String, ISPARQLReturnModifier> lowerMods) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Hashtable<String, ISPARQLReturnModifier> getLowerLevelModifier() {
		// TODO Auto-generated method stub
		return null;
	}



	@Override
	public void setModID(String id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getModID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setModType(SPARQLModifierConstant modConstant) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getModType() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
