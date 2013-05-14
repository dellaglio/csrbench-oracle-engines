package eu.planetdata.srbench.test.csparql;
/*
 * @(#)CounterFormatter.java   1.0   01/ott/2009
 *
 * Copyright 2009-2009 Politecnico di Milano. All Rights Reserved.
 *
 * This software is the proprietary information of Politecnico di Milano.
 * Use is subject to license terms.
 *
 * @(#) $Id$
 */

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.core.ResultFormatter;
import eu.planetdata.srbench.test.gsonUtilities.EngineResults;
import eu.planetdata.srbench.test.gsonUtilities.ExternalBinding;
import eu.planetdata.srbench.test.gsonUtilities.ExternalRelation;
import eu.planetdata.srbench.test.gsonUtilities.Head;
import eu.planetdata.srbench.test.gsonUtilities.InternalBinding;
import eu.planetdata.srbench.test.gsonUtilities.InternalBindingSerializer;
import eu.planetdata.srbench.test.gsonUtilities.InternalRelation;

public class JsonFormatter extends ResultFormatter {

	private EngineResults engineResults = new EngineResults();
	private long timestamp = 0;
	private long stepValue;
	private Gson gson;
	
	private InternalBinding intBinding;
	private HashMap<String, InternalBinding> intBindingMap = new HashMap<String, InternalBinding>();
	
	private ExternalBinding extBinding;
	private ArrayList<ExternalBinding> extBindingList = new ArrayList<ExternalBinding>();
	
	private InternalRelation internalRelation;
	
	private Head head;
	
	private ExternalRelation externalRelation;
	private ArrayList<ExternalRelation> extRelationList = new ArrayList<ExternalRelation>();
	
	private Collection<String> varName;
	private int cont = 0;
	private int testNumber;
	private String jsonOutputDir;
	
	private final static Logger logger = LoggerFactory.getLogger(JsonFormatter.class);
	
	public JsonFormatter(long stepValue, int testNumber, String jsonOutputDir) {
		super();
		this.stepValue = stepValue;
		this.testNumber = testNumber;
		this.jsonOutputDir = jsonOutputDir;
		
		if(!this.jsonOutputDir.endsWith("/"))
			this.jsonOutputDir = this.jsonOutputDir + "/";
		
		gson = new GsonBuilder()
	     .registerTypeAdapter(InternalBinding.class, new InternalBindingSerializer())
	     .create();
		
	}

	@Override
	public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {

		timestamp = timestamp + stepValue;

		String[] field;
		varName = q.getNames();	
		
		
		System.out.println();
		System.out.println("-------"+ q.size() + " results at SystemTime=["+System.currentTimeMillis()+"]--------");
		System.out.println();

		for (final RDFTuple t : q) {

			cont = 0;
			field = t.toString().split("\t");
			
			for(final String s : field){
				
				intBinding = new InternalBinding();
				
				if(s.contains("^^")){
					intBinding.setValue(s.substring(0, s.indexOf("^")).replace("\"", ""));
					intBinding.setType("literal");
					intBinding.setDatatype(s.substring(s.indexOf("^") + 2, s.length()));
				} else {
					intBinding.setValue(s);
					intBinding.setType("uri");
					intBinding.setDatatype("");
				}
				
				intBindingMap.put((String) varName.toArray()[cont], intBinding);
				
				cont++;

			}
			

			extBinding = new ExternalBinding();
			extBinding.setBinding(intBindingMap);
			intBindingMap = new HashMap<String, InternalBinding>();
			extBinding.setTimestamp(timestamp);
			
			extBindingList.add(extBinding);

		}

		internalRelation = new InternalRelation();
		internalRelation.addElements(extBindingList);
		
		extBindingList = new ArrayList<ExternalBinding>();
		head = new Head();
		head.addElements(q.getNames());
		
		externalRelation = new ExternalRelation();
		externalRelation.setTimestamp(timestamp);
		externalRelation.setHead(head);
		externalRelation.setResults(internalRelation);
		extRelationList.add(externalRelation);
		
		engineResults.addElements(extRelationList);
		
		try {
	        BufferedWriter out = new BufferedWriter(new FileWriter(jsonOutputDir + "csparql-answer-" + testNumber + ".json"));
	        out.write(gson.toJson(engineResults));
	        out.close();
	    } catch (IOException e) { 
			logger.error("Error while writing json file", e);
	    }
						
		logger.info(gson.toJson(engineResults));

	}
}
