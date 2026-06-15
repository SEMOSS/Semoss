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
package prerna.ds.export.graph;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.awt.Color;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.ds.TinkerFrame;
import prerna.ds.rdbms.h2.H2Frame;

public class GraphExporterFactoryUnitTests {
	
    @Test
    void testGetExporter() {
    	// test valid tinker frame
        TinkerFrame frame = mock(TinkerFrame.class);
        IGraphExporter exporter = GraphExporterFactory.getExporter(frame);
        assertNotNull(exporter);
        assertTrue(exporter instanceof TinkerFrameGraphExporter);
        
        // test h2Frame
        H2Frame h2Frame = mock(H2Frame.class);
        assertNull(GraphExporterFactory.getExporter(h2Frame));
        
        // null test
        assertNull(GraphExporterFactory.getExporter(null));
    }
    
    @Test
    void testGetExporterColorMap() {
    	// create tinker frame and color map
        TinkerFrame frame = mock(TinkerFrame.class);
        Map<String, Color> colorMap = new HashMap<>();
        colorMap.put("person", Color.RED);

        IGraphExporter exporter = GraphExporterFactory.getExporter(frame, colorMap);
        assertTrue(exporter instanceof TinkerFrameGraphExporter);
        
    	// create h2 frame and color map
        H2Frame h2Frame = mock(H2Frame.class);
        assertNull(GraphExporterFactory.getExporter(h2Frame, colorMap));

    	// null
        assertNull(GraphExporterFactory.getExporter(null, colorMap));

    }
}
