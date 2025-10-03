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
