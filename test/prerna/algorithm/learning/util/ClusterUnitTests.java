package prerna.algorithm.learning.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.*;


public class ClusterUnitTests {
    private Cluster cluster;
    private String[] attributeNames;
    private boolean[] isNumeric;
    private Map<String, Double> categoricalWeights;
    private Map<String, Double> numericalWeights;
    private CategoricalCluster mockCategoricalCluster;
    private NumericalCluster mockNumericalCluster;

    @BeforeEach
    public void setUp() throws Exception {
        attributeNames = new String[]{"attr1", "attr2", "attr3"};
        isNumeric = new boolean[]{true, false, true};
        categoricalWeights = new HashMap<>();
        numericalWeights = new HashMap<>();
        categoricalWeights.put("attr2", 1.0);
        numericalWeights.put("attr1", 1.0);
        numericalWeights.put("attr3", 1.0);

        mockCategoricalCluster = mock(CategoricalCluster.class);
        mockNumericalCluster = mock(NumericalCluster.class);

        cluster = new Cluster(attributeNames, isNumeric);

        setPrivateField(cluster, "categoricalCluster", mockCategoricalCluster);
        setPrivateField(cluster, "numericalCluster", mockNumericalCluster);
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    public void testClusterConstructorWithWeights() throws Exception {
        cluster = new Cluster(categoricalWeights, numericalWeights);

        setPrivateField(cluster, "categoricalCluster", mockCategoricalCluster);
        setPrivateField(cluster, "numericalCluster", mockNumericalCluster);

        assertNotNull(cluster);
    }
    
    @Test
    public void testAddToClusterSingleValueWithFactor() {
        List<Object[]> valuesList = Collections.singletonList(new Object[]{10.0, "A", 30.0});
        cluster.addToCluster(valuesList, attributeNames, isNumeric, 1.0);
        verify(mockNumericalCluster).addToCluster("attr1", 10.0, 1.0);
        verify(mockCategoricalCluster,  times(2)).addToCluster("attr2", "A", 1.0);
        verify(mockNumericalCluster).addToCluster("attr3", 30.0, 1.0);
    }

    @Test
    public void testAddToClusterMultipleValuesWithFactor() {
        List<Object[]> valuesList = Arrays.asList(new Object[]{10.0, "A", 30.0}, new Object[]{20.0, "B", 40.0});
        cluster.addToCluster(valuesList, attributeNames, isNumeric, 1.0);
        verify(mockNumericalCluster).addToCluster("attr1", 15.0, 1.0);
        verify(mockCategoricalCluster).addToCluster("attr2", "A", 1.0);
        verify(mockCategoricalCluster).addToCluster("attr2", "B", 1.0);
        verify(mockNumericalCluster).addToCluster("attr3", 35.0, 1.0);
    }

    @Test
    public void testRemoveFromClusterSingleValueWithFactor() {
        List<Object[]> valuesList = Collections.singletonList(new Object[]{10.0, "A", 30.0});
        cluster.removeFromCluster(valuesList, attributeNames, isNumeric, 1.0);
        verify(mockNumericalCluster).removeFromCluster("attr1", 10.0, 1.0);
        verify(mockCategoricalCluster,  times(2)).removeFromCluster("attr2", "A", 1.0);
        verify(mockNumericalCluster).removeFromCluster("attr3", 30.0, 1.0);
    }

    @Test
    public void testRemoveFromClusterMultipleValuesWithFactor() {
        List<Object[]> valuesList = Arrays.asList(new Object[]{10.0, "A", 30.0}, new Object[]{20.0, "B", 40.0});
        cluster.removeFromCluster(valuesList, attributeNames, isNumeric, 1.0);
        verify(mockNumericalCluster).removeFromCluster("attr1", 15.0, 1.0);
        verify(mockCategoricalCluster).removeFromCluster("attr2", "A", 1.0);
        verify(mockCategoricalCluster).removeFromCluster("attr2", "B", 1.0);
        verify(mockNumericalCluster).removeFromCluster("attr3", 35.0, 1.0);
    }
    //that part
    
    @Test
    public void testAddToClusterSingleValue() {
        List<Object[]> valuesList = Collections.singletonList(new Object[]{10.0, "A", 30.0});
        cluster.addToCluster(valuesList, attributeNames, isNumeric);
        verify(mockNumericalCluster,  times(2)).addToCluster("attr1", 10.0);
        verify(mockCategoricalCluster,  times(2)).addToCluster("attr2", "A", 1.0);
        verify(mockNumericalCluster,  times(2)).addToCluster("attr3", 30.0);
    }

    @Test
    public void testAddToClusterMultipleValues() {
        List<Object[]> valuesList = Arrays.asList(new Object[]{10.0, "A", 30.0}, new Object[]{20.0, "B", 40.0});
        cluster.addToCluster(valuesList, attributeNames, isNumeric);
        verify(mockNumericalCluster).addToCluster("attr1", 15.0);
        verify(mockCategoricalCluster).addToCluster("attr2", "A", 1.0);
        verify(mockCategoricalCluster).addToCluster("attr2", "B", 1.0);
        verify(mockNumericalCluster).addToCluster("attr3", 35.0);
    }

    @Test
    public void testRemoveFromClusterSingleValue() {
        List<Object[]> valuesList = Collections.singletonList(new Object[]{10.0, "A", 30.0});
        cluster.removeFromCluster(valuesList, attributeNames, isNumeric);
        verify(mockNumericalCluster,  times(2)).removeFromCluster("attr1", 10.0);
        verify(mockCategoricalCluster,  times(2)).removeFromCluster("attr2", "A", 1.0);
        verify(mockNumericalCluster,  times(2)).removeFromCluster("attr3", 30.0);
    }

    @Test
    public void testRemoveFromClusterMultipleValues() {
        List<Object[]> valuesList = Arrays.asList(new Object[]{10.0, "A", 30.0}, new Object[]{20.0, "B", 40.0});
        cluster.removeFromCluster(valuesList, attributeNames, isNumeric);
        verify(mockNumericalCluster).removeFromCluster("attr1", 15.0);
        verify(mockCategoricalCluster).removeFromCluster("attr2", "A", 1.0);
        verify(mockCategoricalCluster).removeFromCluster("attr2", "B", 1.0);
        verify(mockNumericalCluster).removeFromCluster("attr3", 35.0);
    }

    @Test
    public void testSetDistanceMode() {
        IClusterDistanceMode mockDistanceMode = mock(IClusterDistanceMode.class);
        cluster.setDistanceMode("attr1", mockDistanceMode);
        verify(mockNumericalCluster).setDistanceMode("attr1", mockDistanceMode);
    }

    @Test
    public void testGetSimilarityForInstance() {
    	List<Object[]> instanceValues = Collections.singletonList(new Object[]{10.0, "A", 30.0});
        when(mockCategoricalCluster.getSimilarity(anyList(), anyList(), anyInt())).thenReturn(0.5);
        when(mockNumericalCluster.getSimilarity(anyList(), anyList(), anyInt())).thenReturn(0.5);
        double similarity = cluster.getSimilarityForInstance(instanceValues, attributeNames, isNumeric, 0);
        assertEquals(0.5, similarity);
    }

    @Test
    public void testGetClusterSimilarity() throws Exception {
        Cluster mockCluster = mock(Cluster.class);

        // Set weights for mockCluster
        CategoricalCluster mockCategoricalCluster2 = mock(CategoricalCluster.class);
        NumericalCluster mockNumericalCluster2 = mock(NumericalCluster.class);
        setPrivateField(mockCluster, "categoricalCluster", mockCategoricalCluster2);
        setPrivateField(mockCluster, "numericalCluster", mockNumericalCluster2);

        when(mockCategoricalCluster.getClusterSimilarity(any(CategoricalCluster.class), anyString())).thenReturn(0.5);
        when(mockNumericalCluster.getClusterSimilarity(any(NumericalCluster.class), anyString())).thenReturn(0.5);

        // Ensure that the weights are not empty
        Map<String, Double> nonEmptyCategoricalWeights = new HashMap<>();
        nonEmptyCategoricalWeights.put("attr2", 1.0);
        Map<String, Double> nonEmptyNumericalWeights = new HashMap<>();
        nonEmptyNumericalWeights.put("attr1", 1.0);
        nonEmptyNumericalWeights.put("attr3", 1.0);

        when(mockCategoricalCluster.getWeights()).thenReturn(nonEmptyCategoricalWeights);
        when(mockNumericalCluster.getWeights()).thenReturn(nonEmptyNumericalWeights);

        when(mockCategoricalCluster2.getWeights()).thenReturn(nonEmptyCategoricalWeights);
        when(mockNumericalCluster2.getWeights()).thenReturn(nonEmptyNumericalWeights);

        double similarity = cluster.getClusterSimilarity(mockCluster, "attr1");
        assertEquals(0.5, similarity);
    }

    @Test
    public void testGetNumInstances() {
        assertEquals(0, cluster.getNumInstances());
    }

    @Test
    public void testSetDistanceModeWithArray() {
        IClusterDistanceMode[] distanceModes = {mock(IClusterDistanceMode.class), mock(IClusterDistanceMode.class), mock(IClusterDistanceMode.class)};
        cluster.setDistanceMode(attributeNames, distanceModes, isNumeric);
        verify(mockNumericalCluster).setDistanceMode("attr1", distanceModes[0]);
        verify(mockNumericalCluster).setDistanceMode("attr3", distanceModes[2]);
    }
    
//    @Test
//    public void testSetDistanceModeWithArrayNoNumeric() {
//        IClusterDistanceMode[] distanceModes = {mock(IClusterDistanceMode.class), mock(IClusterDistanceMode.class), mock(IClusterDistanceMode.class)};
//        cluster.setDistanceMode(attributeNames, distanceModes);
//        verify(mockNumericalCluster).setDistanceMode("attr1", distanceModes[0]);
//        verify(mockNumericalCluster).setDistanceMode("attr3", distanceModes[2]);
//    }
    
    @Test
    public void testSetDistanceModeWithDifferentMeasures() {
        Map<String, IClusterDistanceMode.DistanceMeasure> distanceMeasures = new HashMap<>();
        distanceMeasures.put("attr1", IClusterDistanceMode.DistanceMeasure.MEAN);
        distanceMeasures.put("attr3", IClusterDistanceMode.DistanceMeasure.MEAN);
        cluster.setDistanceMode(distanceMeasures);
        verify(mockNumericalCluster).setDistanceMode(eq("attr1"), any(MeanDistance.class));
        verify(mockNumericalCluster).setDistanceMode(eq("attr3"), any(MeanDistance.class));
    }
}
