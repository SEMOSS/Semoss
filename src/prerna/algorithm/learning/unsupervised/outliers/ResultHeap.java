package prerna.algorithm.learning.unsupervised.outliers;

/**
 * @author Chase
 * 
 * @param <T>
 */
public class ResultHeap<T> {
 private Object[] data;
 private double[] keys;
 private int capacity;
 private int size;
 
 protected ResultHeap(int capacity) {
  this.data = new Object[capacity];
  this.keys = new double[capacity];
  this.capacity = capacity;
  this.size = 0;
 }
 
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
 
 public double getMaxKey() {
  return keys[size - 1];
 }
 
 public Object[] returnData() {
   return data;
 }
 
 @SuppressWarnings("unchecked")
 public T removeMax() {
  if(isEmpty()) return null;
  return (T)data[--size];
 }
 
 public boolean isEmpty() {
  return size == 0;
 }
 
 public boolean isFull() {
  return size == capacity;
 }
 
 public int size() {
  return size;
 }
 
 public int capacity() {
  return capacity;
 }
}