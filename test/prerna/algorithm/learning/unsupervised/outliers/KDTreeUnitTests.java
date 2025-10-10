package prerna.algorithm.learning.unsupervised.outliers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class KDTreeUnitTests {

	 private KDTree<String> kdTree;
	    private final int dimensions = 3;
	    private final int bucketSize = 10;

	    @BeforeEach
	    public void setUp() {
	        kdTree = new KDTree<>(dimensions, bucketSize);
	    }

	    @Test
	    public void testAdd() {
	        double[] key = {1.0, 2.0, 3.0};
	        kdTree.add(key, "A");
	        List<String> result = kdTree.getRange(new double[]{0.0, 0.0, 0.0}, new double[]{2.0, 2.0, 2.0});
	        assertFalse(result.contains("A"));
	    }

	    @Test
	    public void testGetRange() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        List<String> result = kdTree.getRange(new double[]{0.0, 0.0, 0.0}, new double[]{5.0, 5.0, 5.0});
	        assertTrue(result.contains("A"));
	        assertFalse(result.contains("B"));
	    }

	    @Test
	    public void testGetNearestNeighbors() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        ResultHeap<String> result = kdTree.getNearestNeighbors(new double[]{1.0, 2.0, 3.0}, 1);
	        assertEquals("A", result.removeMax());
	    }

	    @Test
	    public void testGetNearestNeighborsWithMoreResults() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        double[] key2 = {4.0, 5.0, 6.0};
	        kdTree.add(key1, "A");
	        kdTree.add(key2, "B");
	        ResultHeap<String> result = kdTree.getNearestNeighbors(new double[]{1.0, 2.0, 3.0}, 2);
	        assertEquals("B", result.removeMax());
	    }

	    @Test
	    public void testGetRangeWithNoResults() {
	        double[] key1 = {1.0, 2.0, 3.0};
	        kdTree.add(key1, "A");
	        List<String> result = kdTree.getRange(new double[]{4.0, 4.0, 4.0}, new double[]{5.0, 5.0, 5.0});
	        assertTrue(result.isEmpty());
	    }
}
