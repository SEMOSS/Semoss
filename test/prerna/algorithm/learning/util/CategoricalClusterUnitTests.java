package prerna.algorithm.learning.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.algorithm.learning.util.CategoricalCluster;

public class CategoricalClusterUnitTests {

	 private CategoricalCluster cluster;

	    @Mock
	    private Map<String, Double> mockWeights;

	    @Before
	    public void setUp() {
	        MockitoAnnotations.openMocks(this);
	        mockWeights = new HashMap<>();
	        mockWeights.put("attribute1", 1.0);
	        mockWeights.put("attribute2", 2.0);
	        cluster = new CategoricalCluster(mockWeights);
	    }

	    @Test
	    public void testAddToClusterSingle() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        assertEquals(1.0, cluster.get("attribute1").get("instance1"), 0.01);
	    }

	    @Test
	    public void testAddToClusterMultiple() {
	        List<String> attributeNames = Arrays.asList("attribute1", "attribute2");
	        List<String> attributeInstances = Arrays.asList("instance1", "instance2");
	        List<Double> values = Arrays.asList(1.0, 2.0);

	        cluster.addToCluster(attributeNames, attributeInstances, values);

	        assertEquals(1.0, cluster.get("attribute1").get("instance1"), 0.01);
	        assertEquals(2.0, cluster.get("attribute2").get("instance2"), 0.01);
	    }

	    @Test
	    public void testRemoveFromClusterSingle() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster.removeFromCluster("attribute1", "instance1", 1.0);
	        assertFalse(cluster.get("attribute1").containsKey("instance1"));
	    }

	    @Test(expected = NullPointerException.class)
	    public void testRemoveFromClusterNonExistent() {
	        cluster.removeFromCluster("attribute1", "instance1", 1.0);
	    }

	    @Test
	    public void testGetSimilarity() {
	        // Implement the logic for getSimilarity method
	        // This is just a placeholder test
	        assertEquals(0.0, cluster.getSimilarity("attribute1", "instance1"), 0.01);
	    }

	    @Test
	    public void testReset() {
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster.reset();
	        assertEquals(0.0, cluster.get("attribute1").get("instance1"), 0.01);
	    }

	    @Test
	    public void testGetClusterSimilarity() {
	        CategoricalCluster cluster2 = new CategoricalCluster(mockWeights);
	        cluster.addToCluster("attribute1", "instance1", 1.0);
	        cluster2.addToCluster("attribute1", "instance1", 1.0);

	        double similarity = cluster.getClusterSimilarity(cluster2, "instanceType");
	        assertEquals(1.0, similarity, 0.01);
	    }

	    @Test
	    public void testGetWeights() {
	        assertEquals(mockWeights, cluster.getWeights());
	    }
}
