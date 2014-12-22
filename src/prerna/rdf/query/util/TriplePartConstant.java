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

public class TriplePartConstant {

	String constant;
	
	//object here because if I put string, anyone method that requires TriplePart, coder can put in string without error
	public TriplePartConstant (Object constant)
	{
		this.constant = (String)constant;
	}
	
	public String getConstant()
	{
		return constant;
	}
	
	public void setConstant(String constant)
	{
		this.constant = constant;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof TriplePartConstant){
			if(this.constant.equals(((TriplePartConstant) obj).constant)){
				return true;
			}
		}
		return false;
	}
}
