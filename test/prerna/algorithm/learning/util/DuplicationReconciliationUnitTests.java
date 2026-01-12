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

public class DuplicationReconciliationUnitTests {
	
	 private DuplicationReconciliation duplicationReconciliation;

	    @BeforeEach
	    public void setUp() {
	        duplicationReconciliation = new DuplicationReconciliation();
	    }

	    @Test
	    public void testGetReconciliatedValueMean() {
	        duplicationReconciliation.addValue(10.0);
	        duplicationReconciliation.addValue(20.0);
	        duplicationReconciliation.addValue(30.0);
	        assertEquals(20.0, duplicationReconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetReconciliatedValueMedian() {
	        DuplicationReconciliation reconciliation = new DuplicationReconciliation(DuplicationReconciliation.ReconciliationMode.MEDIAN);
	        reconciliation.addValue(10.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(30.0);
	        assertEquals(20.0, reconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetReconciliatedValueMode() {
	        DuplicationReconciliation reconciliation = new DuplicationReconciliation(DuplicationReconciliation.ReconciliationMode.MODE);
	        reconciliation.addValue(10.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(30.0);
	        assertEquals(20.0, reconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetReconciliatedValueMax() {
	        DuplicationReconciliation reconciliation = new DuplicationReconciliation(DuplicationReconciliation.ReconciliationMode.MAX);
	        reconciliation.addValue(10.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(30.0);
	        assertEquals(30.0, reconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetReconciliatedValueMin() {
	        DuplicationReconciliation reconciliation = new DuplicationReconciliation(DuplicationReconciliation.ReconciliationMode.MIN);
	        reconciliation.addValue(10.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(30.0);
	        assertEquals(10.0, reconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetReconciliatedValueCount() {
	        DuplicationReconciliation reconciliation = new DuplicationReconciliation(DuplicationReconciliation.ReconciliationMode.COUNT);
	        reconciliation.addValue(10.0);
	        reconciliation.addValue(20.0);
	        reconciliation.addValue(30.0);
	        assertEquals(3.0, reconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testIgnoreEmptyValues() {
	        assertTrue(duplicationReconciliation.ignoreEmptyValues());
	    }

	    @Test
	    public void testSetIgnoreEmptyValues() {
	        duplicationReconciliation.setIgnoreEmptyValues(false);
	        assertTrue(duplicationReconciliation.ignoreEmptyValues());
	    }

	    @Test
	    public void testAddValue() {
	        duplicationReconciliation.addValue(10.0);
	        assertNull(duplicationReconciliation.recValue);
	    }

	    @Test
	    public void testClearValue() {
	        duplicationReconciliation.addValue(10.0);
	        duplicationReconciliation.clearValue();
	        assertEquals(0.0, duplicationReconciliation.getReconciliatedValue());
	    }

	    @Test
	    public void testGetMean() {
	        Object[] values = {10.0, 20.0, 30.0};
	        assertEquals(20.0, DuplicationReconciliation.getMean(values, true));
	    }

	    @Test
	    public void testGetMode() {
	        Object[] values = {10.0, 20.0, 20.0, 30.0};
	        assertEquals(20.0, DuplicationReconciliation.getMode(values, true));
	    }

	    @Test
	    public void testGetMax() {
	        Object[] values = {10.0, 20.0, 30.0};
	        assertEquals(30.0, DuplicationReconciliation.getMax(values, true));
	    }

	    @Test
	    public void testGetMin() {
	        Object[] values = {10.0, 20.0, 30.0};
	        assertEquals(10.0, DuplicationReconciliation.getMin(values, true));
	    }

	    @Test
	    public void testGetMedian() {
	        Object[] values = {10.0, 20.0, 30.0};
	        assertEquals(20.0, DuplicationReconciliation.getMedian(values, true));
	    }
}
