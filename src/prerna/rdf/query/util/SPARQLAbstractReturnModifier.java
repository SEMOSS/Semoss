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
package prerna.rdf.query.util;

import java.util.ArrayList;
import java.util.Hashtable;

public class SPARQLAbstractReturnModifier implements ISPARQLReturnModifier{
	TriplePart triplePart= null;
	String id;
	String type;
	String customModString;
	//hash table for lower mods because generically speaking, you can have multiple mods inside one mod
	//however the generic case, it can only have one modifier 
	public Hashtable<String, ISPARQLReturnModifier> lowerMods = null;
	
	//possible modifiers so far
	public final static SPARQLModifierConstant SUM = new SPARQLModifierConstant("SUM");
	public final static SPARQLModifierConstant DISTINCT = new SPARQLModifierConstant("DISTINCT");
	public final static SPARQLModifierConstant COUNT = new SPARQLModifierConstant("COUNT");
	
	public final static SPARQLModifierConstant AVERAGE = new SPARQLModifierConstant("AVG");
	public final static SPARQLModifierConstant MAX = new SPARQLModifierConstant("MAX");
	public final static SPARQLModifierConstant MIN = new SPARQLModifierConstant("MIN");
	public final static SPARQLModifierConstant NONE = new SPARQLModifierConstant("");
	
	public void createModifier(Object entity, SPARQLModifierConstant type) {
		// TODO Auto-generated method stub
		Hashtable<String, ISPARQLReturnModifier> lowerMods= new Hashtable<String, ISPARQLReturnModifier>();
		if (entity instanceof ISPARQLReturnModifier)
		{
			//always size 0 in that hashtable, so replace with 0
			//generic modifier (eg. COUNT, SUM, DISTINCT) only one type of modifier
			((ISPARQLReturnModifier)entity).setModID(MOD+"0");
			lowerMods.put(MOD+"0", (ISPARQLReturnModifier)entity);
			setLowerLevelModifier(lowerMods);
		}
		else if (entity instanceof TriplePart)
		{
			this.triplePart = (TriplePart) entity;
		}
		else
		{
			throw new IllegalArgumentException("Can only process another SPARQLReturnModifier or another TriplePart");
		}
		setModType(type);
	}
	
	@Override
	public void setLowerLevelModifier(Hashtable<String, ISPARQLReturnModifier> lowerMods) {
		this.lowerMods = lowerMods;
	}

	@Override
	public Hashtable<String, ISPARQLReturnModifier> getLowerLevelModifier() {
		// TODO Auto-generated method stub
		return lowerMods;
	}

	//this class is for generic modifiers only, math modifiers extend this and will override this function
	@Override
	public String getModifierAsString() {
		String modString = "";
		if(lowerMods!=null)
		{
			//don't need to fill really use the ID here because there is only one modifier
			ISPARQLReturnModifier mod = lowerMods.get(MOD+"0");
			modString = type+"("+mod.getModifierAsString()+")";
		}
		else if (triplePart!=null && triplePart.getType().equals(TriplePart.VARIABLE))
		{
			modString = type+"("+SPARQLQueryHelper.createComponentString(triplePart)+")";
		}
		return modString;
	}
	
	public String parameterizeMod(String mod) {
		return "@"+mod+"@";
	}

	@Override
	public void setModID(String id) {
		this.id = id;
	}

	@Override
	public String getModID() {
		return id;
	}

	@Override
	public void setModType(SPARQLModifierConstant modConstant) {
		this.type = modConstant.getConstant();
	}

	@Override
	public String getModType() {
		return type;
	}
	
	
}
