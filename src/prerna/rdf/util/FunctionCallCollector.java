/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.rdf.util;

import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.GroupConcat;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Sample;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;


class FunctionCallCollector extends QueryModelVisitorBase<Exception> {
	public Object value;

	public Object getValue(){
		return value;
	}
	
	@Override
	public void meet(Avg node) {
		System.out.println("Value Avg is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(Count node) {
		System.out.println("Value Count is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(GroupConcat node) {
		System.out.println("GroupConcat is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(Max node) {
		System.out.println("Max is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(Min node) {
		System.out.println("Min is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(Sample node) {
		System.out.println("Sample is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	@Override
	public void meet(Sum node) {
		System.out.println("Sum is  " + node.getArg().getParentNode());
		value = node.getArg().getParentNode();
	}
	
	


}