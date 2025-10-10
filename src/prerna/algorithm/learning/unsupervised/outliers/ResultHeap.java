package prerna.algorithm.learning.unsupervised.outliers;

/**
 * A specialized heap data structure for managing nearest neighbor search results.
 * This heap maintains a sorted collection of results based on distance values,
 * automatically managing capacity and providing efficient access to the results.
 * 
 * <p>
 * The ResultHeap is designed specifically for k-nearest neighbor searches where
 * results need to be kept sorted by distance and the heap has a maximum capacity.
 * It uses insertion sort to maintain order and provides methods to access the
 * maximum distance and remove the farthest result.
 * </p>
 * 
 * @param <T> The type of data objects stored in the heap results.
 * @author Chase
 * @see {@link KDTree#getNearestNeighbors(double[], int)} for primary usage
 */
public class ResultHeap<T> {
 /** Array storing the data objects associated with each result. */
 private Object[] data;
 
 /** Array storing the distance keys (usually squared distances) for each result. */
 private double[] keys;
 
 /** Maximum number of results this heap can hold. */
 private int capacity;
 
 /** Current number of results stored in the heap. */
 private int size;
 
 /**
  * Constructs a result heap with the specified capacity.
  * 
  * @param capacity The maximum number of results this heap can hold.
  */
 protected ResultHeap(int capacity) {
  this.data = new Object[capacity];
  this.keys = new double[capacity];
  this.capacity = capacity;
  this.size = 0;
 }
 
 /**
  * Offers a new result to the heap, maintaining sorted order by distance.
  * If the heap is at capacity and the new key is larger than the maximum,
  * the offer is ignored. Otherwise, the result is inserted in sorted order.
  * 
  * @param key The distance key for this result (usually squared distance).
  * @param value The data object associated with this result.
  */
 protected void offer(double key, T value) {
  int i = size;
  for (; i > 0 && keys[i - 1] > key; --i);
  if (i >= capacity) return;
  if (size < capacity) ++size;
  int j = i + 1;
  System.arraycopy(keys, i, keys, j, size - j);
  keys[i] = key;
  System.arraycopy(data, i, data, j, size - j);
  data[i] = value;
 }
 
 /**
  * Gets the maximum distance key currently stored in the heap.
  * 
  * @return The largest distance key in the heap.
  */
 public double getMaxKey() {
  return keys[size - 1];
 }
 
 /**
  * Returns the internal data array containing all result objects.
  * 
  * @return Array of data objects stored in the heap.
  */
 public Object[] returnData() {
   return data;
 }
 
 /**
  * Removes and returns the result with the maximum distance key.
  * 
  * @return The data object with the largest distance, or null if the heap is empty.
  */
 @SuppressWarnings("unchecked")
 public T removeMax() {
  if(isEmpty()) return null;
  return (T)data[--size];
 }
 
 /**
  * Determines if the heap is currently empty.
  * 
  * @return True if the heap contains no results, false otherwise.
  */
 public boolean isEmpty() {
  return size == 0;
 }
 
 /**
  * Determines if the heap has reached its maximum capacity.
  * 
  * @return True if the heap is full, false otherwise.
  */
 public boolean isFull() {
  return size == capacity;
 }
 
 /**
  * Gets the current number of results stored in the heap.
  * 
  * @return The number of results currently in the heap.
  */
 public int size() {
  return size;
 }
 
 /**
  * Gets the maximum capacity of the heap.
  * 
  * @return The maximum number of results this heap can hold.
  */
 public int capacity() {
  return capacity;
 }
}