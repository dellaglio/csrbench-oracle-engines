package eu.planetdata.srbench.test.gsonUtilities;

import java.util.Collection;

public class Head {
	
	private String[] vars;

	public String[] getVars() {
		return vars;
	}

	public void setVars(String[] vars) {
		this.vars = vars;
	}
	
	public void addElements(Collection<String> list){
		vars = new String[list.size()];
		list.toArray(vars);
	}

}
