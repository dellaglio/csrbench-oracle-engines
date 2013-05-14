package eu.planetdata.csrbench.test.gsonUtilities;

import java.util.Collection;

public class InternalRelation {
	
	private ExternalBinding[] bindings;

	public ExternalBinding[] getBindings() {
		return bindings;
	}

	public void setBindings(ExternalBinding[] bindings) {
		this.bindings = bindings;
	}
	
	public void addElements(Collection<ExternalBinding> list){
		bindings = new ExternalBinding[list.size()];
		list.toArray(bindings);
	}

}
