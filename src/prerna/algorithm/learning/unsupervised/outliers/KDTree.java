package prerna.algorithm.learning.unsupervised.outliers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A K-Dimensional Bucket Tree implementation for fast sorting and searching of K-dimensional data.
 * This data structure provides efficient nearest neighbor searches and range queries in
 * multi-dimensional space using a tree-based partitioning approach.
 * 
 * <p>
 * The KDTree uses a bucket-based approach where leaf nodes can contain multiple data points
 * up to a specified bucket size. This reduces tree depth and improves performance for
 * datasets with moderate dimensionality. The tree automatically partitions space by
 * selecting the dimension with the largest range at each split.
 * </p>
 * 
 * <p>
 * This implementation supports:
 * <ul>
 * <li>Efficient nearest neighbor searches with {@link #getNearestNeighbors(double[], int)}</li>
 * <li>Range queries with {@link #getRange(double[], double[])}</li>
 * <li>Dynamic insertion of new data points with {@link #add(double[], Object)}</li>
 * </ul>
 * </p>
 * 
 * @param <T> The type of values associated with each data point in the tree.
 * @author Chase
 * @see {@link ResultHeap} for nearest neighbor result management
 */
public class KDTree<T> {
	/** The default bucket size for leaf nodes when not specified in constructor. */
	protected static final int defaultBucketSize = 48;

	/** The number of dimensions in the data points stored in this tree. */
	private final int dimensions;
	
	/** The maximum number of data points that can be stored in a leaf node. */
	private final int bucketSize;
	
	/** The root node of the KD-Tree. */
	private NodeKD root;

	/**
	 * Constructs a KD-Tree with the specified number of dimensions and default bucket size.
	 * 
	 * @param dimensions The number of dimensions for data points in this tree.
	 */
	public KDTree(int dimensions) {
		this.dimensions = dimensions;
		this.bucketSize = defaultBucketSize;
		this.root = new NodeKD();
	}

	/**
	 * Constructs a KD-Tree with the specified number of dimensions and custom bucket size.
	 * 
	 * @param dimensions The number of dimensions for data points in this tree.
	 * @param bucket The maximum size of the buckets in leaf nodes.
	 */
	public KDTree(int dimensions, int bucket) {
		this.dimensions = dimensions;
		this.bucketSize = bucket;
		this.root = new NodeKD();
	}

	/**
	 * Adds a data point and its associated value to the tree.
	 * 
	 * @param key The coordinate array representing the data point location.
	 * @param val The object to associate with this data point.
	 */
	public void add(double[] key, T val) {
		root.addPoint(key, val);
	}

	/**
	 * Returns all data points within the specified range defined by lower and upper bounds.
	 * The range is inclusive of the boundary values.
	 * 
	 * @param low The lower bounds of the search area for each dimension.
	 * @param high The upper bounds of the search area for each dimension.
	 * @return List of all values whose associated points fall within the specified range.
	 */
	@SuppressWarnings("unchecked")
	public List<T> getRange(double[] low, double[] high) {
		Object[] objs = root.range(high, low);
		ArrayList<T> range = new ArrayList<T>(objs.length);
		for(int i=0; i<objs.length; ++i) {
			range.add((T)objs[i]);
		}
		return range;
	}

	/**
	 * Gets the N nearest neighbors to the specified data point.
	 * 
	 * @param key The coordinate array representing the query point.
	 * @param num The maximum number of nearest neighbors to return.
	 * @return A {@link ResultHeap} containing the nearest neighbors, where distances 
	 *         are the square of the actual Euclidean distance.
	 */
	public ResultHeap<T> getNearestNeighbors(double[] key, int num) {
		ResultHeap<T> heap = new ResultHeap<T>(num);
		root.nearest(heap, key);
		return heap;
	}


	/**
	 * Internal tree node class that represents both leaf and internal nodes in the KD-Tree.
	 * Leaf nodes store data points in buckets, while internal nodes store partitioning information.
	 */
	private class NodeKD {
		/** Left child node for points with coordinates less than or equal to the slice value. */
		private NodeKD left, right;
		
		/** Bounding box coordinates representing the maximum and minimum bounds of this node's region. */
		private double[] maxBounds, minBounds;
		
		/** Array storing the values associated with data points in this leaf node. */
		private Object[] bucketValues;
		
		/** Array storing the coordinate arrays of data points in this leaf node. */
		private double[][] bucketKeys;
		
		/** Flag indicating whether this node is a leaf node (true) or internal node (false). */
		private boolean isLeaf;
		
		/** Current number of data points stored in this leaf node and the dimension used for partitioning. */
		private int current, sliceDimension;
		
		/** The coordinate value used to partition data points between left and right child nodes. */
		private double slice;

		/**
		 * Constructs a new leaf node with empty buckets and default settings.
		 */
		private NodeKD() {
			bucketValues = new Object[bucketSize];
			bucketKeys = new double[bucketSize][];

			left = right = null;
			maxBounds = minBounds = null;

			isLeaf = true;

			current = 0;
		}

		/**
		 * Adds a data point to this node or routes it to the appropriate child node.
		 * 
		 * @param key The coordinate array of the data point to add.
		 * @param val The value associated with the data point.
		 */
		private void addPoint(double[] key, Object val) {
			if(isLeaf) {
				addLeafPoint(key,val);
			} else {
				extendBounds(key);
				if (key[sliceDimension] > slice) {
					right.addPoint(key, val);
				} else {
					left.addPoint(key, val);
				}
			}
		}

		/**
		 * Adds a data point to this leaf node, splitting the node if it exceeds bucket capacity.
		 * 
		 * @param key The coordinate array of the data point to add.
		 * @param val The value associated with the data point.
		 */
		private void addLeafPoint(double[] key, Object val) {
			extendBounds(key);
			if (current + 1 > bucketSize) {
				splitLeaf();
				addPoint(key, val);
				return;
			}
			bucketKeys[current] = key;
			bucketValues[current] = val;
			++current;
		}

		/**
		 * Find the nearest neighbor recursively.
		 */
		@SuppressWarnings("unchecked")
		private void nearest(ResultHeap<T> heap, double[] data) {
			if(current == 0)
				return;
			if(isLeaf) {
				//IS LEAF
				for (int i = 0; i < current; ++i) {
					double dist = pointDistSq(bucketKeys[i], data);
					heap.offer(dist, (T) bucketValues[i]);
				}
			} else {
				//IS BRANCH
				if (data[sliceDimension] > slice) {
					right.nearest(heap, data);
					if(left.current == 0)
						return;
					if (!heap.isFull() || regionDistSq(data,left.minBounds,left.maxBounds) < heap.getMaxKey()) {
						left.nearest(heap, data);
					}
				} else {
					left.nearest(heap, data);
					if (right.current == 0)
						return;
					if (!heap.isFull() || regionDistSq(data,right.minBounds,right.maxBounds) < heap.getMaxKey()) {
						right.nearest(heap, data);
					}
				}
			}
		}

		// gets all items from within a range
		private Object[] range(double[] upper, double[] lower) {
			if (bucketValues == null) {
				// Branch
				Object[] tmp = new Object[0];
				if (intersects(upper, lower, left.maxBounds, left.minBounds)) {
					Object[] tmpl = left.range(upper, lower);
					if (0 == tmp.length) tmp = tmpl;
				}
				if (intersects(upper, lower, right.maxBounds, right.minBounds)) {
					Object[] tmpr = right.range(upper, lower);
					if (0 == tmp.length)
						tmp = tmpr;
					else if (0 < tmpr.length) {
						Object[] tmp2 = new Object[tmp.length + tmpr.length];
						System.arraycopy(tmp, 0, tmp2, 0, tmp.length);
						System.arraycopy(tmpr, 0, tmp2, tmp.length, tmpr.length);
						tmp = tmp2;
					}
				}
				return tmp;
			}
			// Leaf
			Object[] tmp = new Object[current];
			int n = 0;
			for (int i = 0; i < current; ++i) {
				if (contains(upper, lower, bucketKeys[i])) {
					tmp[n++] = bucketValues[i];
				}
			}
			Object[] tmp2 = new Object[n];
			System.arraycopy(tmp, 0, tmp2, 0, n);
			return tmp2;
		}

		// These are helper functions from here down
		// check if this hyper rectangle contains a give hyper-point
		public boolean contains(double[] upper, double[] lower, double[] point) {
			if (current == 0) return false;
			for (int i = 0; i < point.length; ++i) {
				if (point[i] > upper[i] || point[i] < lower[i]) return false;
			}
			return true;
		}

		// checks if two hyper-rectangles intersect
		public boolean intersects(double[] up0, double[] low0, double[] up1, double[] low1) {
			for (int i = 0; i < up0.length; ++i) {
				if (up1[i] < low0[i] || low1[i] > up0[i]) return false;
			}
			return true;
		}

		private void splitLeaf() {
			double bestRange = 0;
			for(int i=0;i<dimensions;++i) {
				double range = maxBounds[i] - minBounds[i];
				if(range > bestRange) {
					sliceDimension = i;
					bestRange = range;
				}
			}

			left = new NodeKD();
			right = new NodeKD();

			slice = (maxBounds[sliceDimension] + minBounds[sliceDimension]) * 0.5;

			for (int i = 0; i < current; ++i) {
				if (bucketKeys[i][sliceDimension] > slice) {
					right.addLeafPoint(bucketKeys[i], bucketValues[i]);
				} else {
					left.addLeafPoint(bucketKeys[i], bucketValues[i]);
				}
			}
			bucketKeys = null;
			bucketValues = null;
			isLeaf = false;
		}

		// expands this hyper rectangle
		private void extendBounds(double[] key) {
			if (maxBounds == null) {
				maxBounds = Arrays.copyOf(key, dimensions);
				minBounds = Arrays.copyOf(key, dimensions);
				return;
			}
			for (int i = 0; i < key.length; ++i) {
				if (maxBounds[i] < key[i]) maxBounds[i] = key[i];
				if (minBounds[i] > key[i]) minBounds[i] = key[i];
			}
		}
	}


	private static final double pointDistSq(double[] p1, double[] p2) {
		double d = 0;
		double q = 0;
		for (int i = 0; i < p1.length; ++i) {
			d += (q=(p1[i] - p2[i]))*q;
		}
		return d;
	}

	private static final double regionDistSq(double[] point, double[] min, double[] max) {
		double d = 0;
		double q = 0;
		for (int i = 0; i < point.length; ++i) {
			if (point[i] > max[i]) {
				d += (q = (point[i] - max[i]))*q;
			} else if (point[i] < min[i]) {
				d += (q = (point[i] - min[i]))*q;
			}
		}
		return d;
	}
}