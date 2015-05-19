package prerna.algorithm.learning.unsupervised.recommender;

/*
 * class to hold important data for a user
 */
public class User implements Comparable<User>{
	
    private String id;
    private double simScore;
    
    protected User(String s, double d) {
        id = s;
        simScore = d;
    }
    
    protected User(String s){
        id = s;
        simScore = 0.0;
    }
    
    public void setSimilarity(double sim){
        simScore = sim;
    }
    
    public String getID() {
        return id;
    }
    
    public double getSimilarity() {
        return simScore;
    }


	@Override
	public int compareTo(User u) {
		//ascending order
		//if(this.getSimilarity() > u.getSimilarity()) return 1;
		
		//descending order
		if(this.getSimilarity() < u.getSimilarity()) return 1;
		
		else return -1;
	}

}
