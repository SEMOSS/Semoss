package prerna.engine.impl.vector;

public class FileEmbeddingStatus {
	private String fileName;
    private String status;
    private long insertedRecords;
    private long failedRecords;
    private long totalRecords;

    public FileEmbeddingStatus() {}

    public FileEmbeddingStatus(String fileName, String status, long insertedRecords, long failedRecords, long totalRecords) {
    	this.fileName = fileName;
        this.status = status;
        this.insertedRecords = insertedRecords;
        this.failedRecords = failedRecords;
        this.totalRecords = totalRecords;
    }
    
    public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getInsertedRecords() {
		return insertedRecords;
	}

	public void setInsertedRecords(long insertedRecords) {
		this.insertedRecords = insertedRecords;
	}

	public long getFailedRecords() {
		return failedRecords;
	}

	public void setFailedRecords(long failedRecords) {
		this.failedRecords = failedRecords;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

   
}
