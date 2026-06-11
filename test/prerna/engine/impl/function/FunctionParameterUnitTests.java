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
package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class FunctionParameterUnitTests {
	
	@Test
	void testFunctionParameterConstructor1() {
		String testParameterName = "test param name";
		String testParameterType = "test param type";
		String testParameterDescription = "test param desc";
		FunctionParameter fP = new FunctionParameter();
		assertNull(fP.getParameterName());
		assertNull(fP.getParameterType());
		assertNull(fP.getParameterDescription());
		
		fP.setParameterName(testParameterName);
		fP.setParameterType(testParameterType);
		fP.setParameterDescription(testParameterDescription);
		assertNotNull(fP.getParameterName());
		assertNotNull(fP.getParameterType());
		assertNotNull(fP.getParameterDescription());
		assertEquals(testParameterName, fP.getParameterName());
		assertEquals(testParameterType, fP.getParameterType());
		assertEquals(testParameterDescription, fP.getParameterDescription());
	}
	
	@Test
	void testFunctionParameterConstructor2() {
		String testParameterName = "test param name";
		String testParameterType = "test param type";
		String testParameterDescription = "test param desc";
		FunctionParameter fP = new FunctionParameter(testParameterName, testParameterType, testParameterDescription);
		assertNotNull(fP.getParameterName());
		assertNotNull(fP.getParameterType());
		assertNotNull(fP.getParameterDescription());
		assertEquals(testParameterName, fP.getParameterName());
		assertEquals(testParameterType, fP.getParameterType());
		assertEquals(testParameterDescription, fP.getParameterDescription());
	}
	
	@Test
	void testSetParamMethods() {
		String testParameterName = "test param name";
		String testParameterType = "test param type";
		String testParameterDescription = "test param desc";
		FunctionParameter fP = new FunctionParameter(testParameterName, testParameterType, testParameterDescription);
		assertNotNull(fP.getParameterName());
		assertNotNull(fP.getParameterType());
		assertNotNull(fP.getParameterDescription());
		assertEquals(testParameterName, fP.getParameterName());
		assertEquals(testParameterType, fP.getParameterType());
		assertEquals(testParameterDescription, fP.getParameterDescription());
		
		String testParameterName2 = "test param name 2";
		String testParameterType2 = "test param type 2";
		String testParameterDescription2 = "test param desc 2";
		fP.setParameterName(testParameterName2);
		fP.setParameterType(testParameterType2);
		fP.setParameterDescription(testParameterDescription2);
		assertEquals(testParameterName2, fP.getParameterName());
		assertEquals(testParameterType2, fP.getParameterType());
		assertEquals(testParameterDescription2, fP.getParameterDescription());
	}
}
