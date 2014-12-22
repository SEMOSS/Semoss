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

public class SEMOSSParameter {
	private String paramType;
	private String param;
	private TriplePart paramPart;
	
	String parameterString;
	
	SEMOSSParameter(String param, String paramType, TriplePart paramPart){
		this.param = param;
		this.paramType = paramType;
		this.paramPart = paramPart;
	}
	
	public void createString() {
		String objectString = SPARQLQueryHelper.createComponentString(paramPart);
		parameterString = "BIND(<@" + param + "-" + paramType + "@> AS " + objectString + ")";
	}
	
	public String getParamString() {
		createString();
		return parameterString;
	}
	
	public String getParamName() {
		return param;
	}
	
	public String getParamType() {
		return paramType;
	}
	
	public TriplePart getParamPart() {
		return paramPart;
	}
}
