package eu.planetdata.csrbench.test.gsonUtilities;

public class ExternalRelation {
	
	private Head head;
	private long timestamp;
	private InternalRelation results;
	
	public Head getHead() {
		return head;
	}
	public void setHead(Head head) {
		this.head = head;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public InternalRelation getResults() {
		return results;
	}
	public void setResults(InternalRelation results) {
		this.results = results;
	}

}
