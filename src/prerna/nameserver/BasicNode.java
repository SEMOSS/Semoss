/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
