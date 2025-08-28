/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.algorithm.learning.unsupervised.outliers;

/**
 * @author Chase
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
		if (i >= capacity)
			return;
		if (size < capacity)
			++size;
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
		if (isEmpty())
			return null;
		return (T) data[--size];
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
