package eu.planetdata.srbench.test.gsonUtilities;

import java.util.Collection;

public class EngineResults {
	
	private ExternalRelation[] relations;

	public ExternalRelation[] getRelations() {
		return relations;
	}

	public void setRelations(ExternalRelation[] relations) {
		this.relations = relations;
	}
	
	public void addElements(Collection<ExternalRelation> list){
		relations = new ExternalRelation[list.size()];
		list.toArray(relations);
	}

}
