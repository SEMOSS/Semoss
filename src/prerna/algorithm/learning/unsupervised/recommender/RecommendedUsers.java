package prerna.algorithm.learning.unsupervised.recommender;

import java.util.ArrayList;

/*
 * This class handles the top recommended users
 */
public class RecommendedUsers {

    private ArrayList<User> userList; 
    private int capacity;
    private int lastUser;
    private double threshold;
    
    protected RecommendedUsers() {
        capacity = 100;
        userList = new ArrayList<>(capacity);
        lastUser = -1;
        threshold = 0.5;
    }
    
    protected RecommendedUsers(int c) {
        capacity = c;
        userList = new ArrayList<>(capacity);
        lastUser = -1;
        threshold = 0.5;
    }
    
    protected void insert(User u) {
    	//below capacity?
        if(lastUser<capacity){
        	//should only put users in the list if they meet a certain threshold
        	if(u.getSimilarity()>=threshold){
        		userList.add(u);
        		lastUser++;
        	}
        } else {
        	//is score better than least similar user?
        	if(userList.get(lastUser).getSimilarity() < u.getSimilarity()) {
            	//place user in the right spot
        		//potentially minimize looping based on similarity value
            	for(int i = capacity; i >= 0; i--) {
            		User nextUser = userList.get(i);
            		if(nextUser.getSimilarity() < u.getSimilarity()) {
            			userList.add(i,u); //add the new user
            			userList.remove(lastUser+1);//remove the user pushed out of top users
            			break;
            		}
            	}
            }
        }        
    }
    
    protected void insert(String id, double similarity) {
        User u = new User(id, similarity);
        insert(u);
    }
    

    //Search the userList and remove the user associated with the id param if one exists
    protected void remove(String id) {
    	for(int i=0; i<=lastUser; i++){
    		if(userList.get(i).getID().equals(id)){
    			userList.remove(i);
    			lastUser--;
    			break;
    		}
    	}
    }
    
    protected String[] getTop(int i) {
    	if(lastUser<0) {
    		return null;
    	}
    	if (i>lastUser) {
    		i = lastUser;
    	}
    	
        String[] retValues = new String[i];
        for(int index=0; index<=i; i++){
        	retValues[index] = userList.get(index).getID();
        }
        
    	return retValues;
    }
    
    protected String[] getAllUsers() {
    	return getTop(lastUser);
    }
    
    protected String getLeastSimilarUser() {
    	return userList.get(lastUser).getID();
    }
    
    protected double getLeastSimilarScore() {
    	return userList.get(lastUser).getSimilarity();
    }
    
    protected void setCapacity(int c) {
        capacity = c;
    }
    
    protected int getCapacity(){
        return capacity;
    }
    
    protected void setThreshold(double t) {
    	threshold = t;
    }
     
}

