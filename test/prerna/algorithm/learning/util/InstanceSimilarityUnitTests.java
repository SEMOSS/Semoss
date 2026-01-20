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
package prerna.algorithm.learning.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InstanceSimilarityUnitTests {
	 private List<Object[]> instance1;
	    private List<Object[]> instance2;
	    private boolean[] isNumeric;
	    private String[] attributeNames;
	    private Map<String, DuplicationReconciliation> dups;

	    @BeforeEach
	    public void setUp() {
	        instance1 = new ArrayList<>();
	        instance2 = new ArrayList<>();
	        isNumeric = new boolean[]{true, false, true};
	        attributeNames = new String[]{"attr1", "attr2", "attr3"};
	        dups = new HashMap<>();

	        DuplicationReconciliation mockDup1 = mock(DuplicationReconciliation.class);
	        DuplicationReconciliation mockDup2 = mock(DuplicationReconciliation.class);
	        DuplicationReconciliation mockDup3 = mock(DuplicationReconciliation.class);

	        when(mockDup1.getReconciliatedValue()).thenReturn(10.0);
	        when(mockDup2.getReconciliatedValue()).thenReturn(20.0);
	        when(mockDup3.getReconciliatedValue()).thenReturn(30.0);

	        dups.put("attr1", mockDup1);
	        dups.put("attr2", mockDup2);
	        dups.put("attr3", mockDup3);

	        instance1.add(new Object[]{10.0, "A", 30.0});
	        instance2.add(new Object[]{10.0, "A", 30.0});
	    }

	    @Test
	    public void testGetInstanceSimilarity() {
	        double similarity = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(1.0, similarity);
	    }

	    @Test
	    public void testCalculateNumericalSim() {
	        double numericalSim = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(1.0, numericalSim);
	    }

	    @Test
	    public void testCalculateInstanceCategoricalSim() {
	        instance1.add(new Object[]{10.0, "B", 30.0});
	        instance2.add(new Object[]{10.0, "B", 30.0});
	        double categoricalSim = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(0.8333333333333333, categoricalSim);
	    }

	    @Test
	    public void testCalculateNumericalSimWithDifferentValues() {
	        instance1.add(new Object[]{15.0, "A", 35.0});
	        instance2.add(new Object[]{20.0, "A", 40.0});
	        double numericalSim = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(1.0, numericalSim);
	    }

	    @Test
	    public void testCalculateInstanceCategoricalSimWithDifferentValues() {
	        instance1.add(new Object[]{10.0, "B", 30.0});
	        instance2.add(new Object[]{10.0, "C", 30.0});
	        double categoricalSim = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(0.75, categoricalSim);
	    }

	    @Test
	    public void testCalculateNumericalSimWithEmptyValues() {
	        instance1.add(new Object[]{null, "A", 30.0});
	        instance2.add(new Object[]{null, "A", 30.0});
	        double numericalSim = InstanceSimilarity.getInstanceSimilarity(instance1, instance2, isNumeric, attributeNames, dups);
	        assertEquals(1.0, numericalSim);
	    }
}
