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

import static org.junit.jupiter.api.Assertions.*;

import java.awt.Color;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class IGraphExporterUnitTests {

    @Nested
    class GetRgbTests {

        @Test void red() {
            assertEquals("255,0,0", IGraphExporter.getRgb(Color.RED));
        }

        @Test void green() {
            assertEquals("0,255,0", IGraphExporter.getRgb(Color.GREEN));
        }

        @Test void blue() {
            assertEquals("0,0,255", IGraphExporter.getRgb(Color.BLUE));
        }

        @Test void white() {
            assertEquals("255,255,255", IGraphExporter.getRgb(Color.WHITE));
        }

        @Test void black() {
            assertEquals("0,0,0", IGraphExporter.getRgb(Color.BLACK));
        }

        @Test void yellow() {
            assertEquals("255,255,0", IGraphExporter.getRgb(Color.YELLOW));
        }

        @Test void customColor() {
            Color custom = new Color(128, 64, 32);
            assertEquals("128,64,32", IGraphExporter.getRgb(custom));
        }

        @Test void gray() {
            assertEquals("128,128,128", IGraphExporter.getRgb(Color.GRAY));
        }

        @Test void cyan() {
            assertEquals("0,255,255", IGraphExporter.getRgb(Color.CYAN));
        }

        @Test void magenta() {
            assertEquals("255,0,255", IGraphExporter.getRgb(Color.MAGENTA));
        }

        @Test void formatIsCommaDelimited() {
            String rgb = IGraphExporter.getRgb(new Color(1, 2, 3));
            String[] parts = rgb.split(",");
            assertEquals(3, parts.length);
            assertEquals("1", parts[0]);
            assertEquals("2", parts[1]);
            assertEquals("3", parts[2]);
        }

        @Test void boundaryValues_min() {
            assertEquals("0,0,0", IGraphExporter.getRgb(new Color(0, 0, 0)));
        }

        @Test void boundaryValues_max() {
            assertEquals("255,255,255", IGraphExporter.getRgb(new Color(255, 255, 255)));
        }

        @Test void orange() {
            assertEquals("255,200,0", IGraphExporter.getRgb(Color.ORANGE));
        }

        @Test void pink() {
            assertEquals("255,175,175", IGraphExporter.getRgb(Color.PINK));
        }
    }
}
