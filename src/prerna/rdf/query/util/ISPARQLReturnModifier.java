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

public interface ISPARQLReturnModifier {
	
	//this interface API does not support two modifiers on one level
	public final static String MOD = "MODIFIER";
	
	public void setLowerLevelModifier(Hashtable<String, ISPARQLReturnModifier> lowerMods);
	
	public Hashtable<String, ISPARQLReturnModifier> getLowerLevelModifier();
	
	public String getModifierAsString();
	
	public void setModID(String id);
	
	public String getModID();
	
	public void setModType(SPARQLModifierConstant modConstant);
	
	public String getModType();
	
}
