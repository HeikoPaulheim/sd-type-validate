package de.dwslab.sdtv;

import java.io.IOException;

/**
 * The main class that runs SDType&Validate
 * @author Heiko
 */
public class Runner {
	public static void main(String[] args) {
		LoadFiles loadFiles = new LoadFiles();
		ComputeBaseStatistics computeBaseStatistics = new ComputeBaseStatistics();
		MaterializeSDTypes materializeSDTypes = new MaterializeSDTypes();
		MaterializeSDValidate materializeSDValidate = new MaterializeSDValidate();
		try {
			loadFiles.loadProperties("./enwiki-20151002-mappingbased-objects-uncleaned.ttl");
			loadFiles.createPropertyIndices();
			loadFiles.loadTypes("./enwiki-20151002-instance-types-transitive.ttl");
			loadFiles.createTypeIndices();
			loadFiles.loadDisambiguations("./enwiki-20151002-disambiguations-unredirected.ttl");
			loadFiles.createDisambiguationIndices();
			computeBaseStatistics.computeGlobalTypeDistribution();
			computeBaseStatistics.computePerPredicateDistribution();
			materializeSDTypes.computeSDTypes();
			materializeSDTypes.writeTypeFile("./sdtypes.ttl", 0.4f);
			materializeSDValidate.computeSDValidateScores();
			materializeSDValidate.writeWrongStatementsFile("./sdinvalid.ttl", 0.15f);
			if(1<0)
				throw new IOException();
		} catch (IOException e) {
			System.out.println("Error loading input files");
			e.printStackTrace();
		}
	}
}
