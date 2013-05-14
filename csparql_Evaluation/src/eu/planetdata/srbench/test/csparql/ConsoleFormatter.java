package eu.planetdata.srbench.test.csparql;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.core.ResultFormatter;

public class ConsoleFormatter extends ResultFormatter {

	@Override
	public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
		System.out.println("-------"+ q.size() + " results at SystemTime=["+System.currentTimeMillis()+"]--------");
		for (final RDFTuple t : q) {
			System.out.println(t.toString());
		}

	}
}
