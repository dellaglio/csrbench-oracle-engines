package eu.planetdata.srbench.test.gsonUtilities;

import java.util.HashMap;

public class ExternalBinding {
	
	private long timestamp;
	private HashMap<String, InternalBinding> binding;
	
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public HashMap<String, InternalBinding> getBinding() {
		return binding;
	}
	public void setBinding(HashMap<String, InternalBinding> binding) {
		this.binding = binding;
	}
	
}
