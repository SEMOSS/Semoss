package prerna.nameserver;

/**
 * Basic node meant to be extended for TreeNode.java and BipartiteNode.java
 * @param <T>
 */
public abstract class BasicNode<T> {

	// the value of the node
	protected T data;
	
	public BasicNode(T data) {
		this.data = data;
	}
	
	/**
	 * @return Returns the value of the node
	 */
	public T getData() {
		return data;
	}

	/**
	 * Sets the value of the node
	 * @param data		Value of the node
	 */
	public void setData(T data) {
		this.data = data;
	}
	
	/**
	 * Return the value of the node as the string
	 */
	@Override
	public String toString() {
		return this.data.toString();
	}
	
	/**
	 * Return true if the two objects are the same type and have the same data value
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof BasicNode)) {
			return false;
		}
		if(obj == this) {
			return true;
		}
		
		BasicNode<T> node = (BasicNode<T>) obj;
		if(!this.data.getClass().equals(node.data.getClass())) {
			return false;
		}
		
		if(this.data.toString().equals(node.data.toString())) {
			return true;
		}
		return false;
	}
}
