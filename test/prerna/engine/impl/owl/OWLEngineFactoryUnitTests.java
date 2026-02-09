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
package prerna.engine.impl.owl;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.util.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class OWLEngineFactoryUnitTests {

    private OWLEngineFactory factory;

    @Test
    void testGetReadOWL() {

        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            RDFFileSesameEngine rfse = mock(RDFFileSesameEngine.class);

            Vector<String> empty = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(empty);
            Vector<String[]> emptyArray = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorArrayOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(emptyArray);

            factory = new OWLEngineFactory(rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engineId", "engineName");

            ReadOnlyOWLEngine engine = factory.getReadOWL();
            assertEquals("engineId", engine.engineId);
            assertEquals("engineName", engine.engineName);
        }

    }

    @Test
    void testGetWriteOWL() throws InterruptedException {
        try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
            RDFFileSesameEngine rfse = mock(RDFFileSesameEngine.class);

            Vector<String> empty = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(empty);
            Vector<String[]> emptyArray = new Vector<>();
            utilityMockedStatic.when(() -> Utility.getVectorArrayOfReturn(anyString(), eq(rfse), eq(true))).thenReturn(emptyArray);

            factory = new OWLEngineFactory(rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engineId", "engineName");

            WriteOWLEngine engine = factory.getWriteOWL();
            assertEquals("engineId", engine.engineId);
            assertEquals("engineName", engine.engineName);
        }
    }
}
