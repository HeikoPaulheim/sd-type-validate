package de.dwslab.sdtv;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.didion.jwnl.JWNL;
import net.didion.jwnl.JWNLException;
import net.didion.jwnl.data.IndexWord;
import net.didion.jwnl.data.POS;
import net.didion.jwnl.data.Synset;
import net.didion.jwnl.data.Word;
import net.didion.jwnl.dictionary.Dictionary;

public class MaterializeSDTypes {
	private int chunkSize = 1000;
	
	public void computeSDTypes() {
		System.out.println("Computing SDTypes");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error preparing SDTypes");
			e.printStackTrace();
		}
		String create="";
		String insert="";
		
		create = "CREATE  TABLE dbpedia_untyped_instance (resource VARCHAR(1000))";
		Util.removeTableIfExisting("dbpedia_untyped_instance");
		try {
			stmt.execute(create);
		} catch (SQLException e) {
			System.out.println("Error creating untyped instance table");
			e.printStackTrace();
		}
		
		insert = "INSERT INTO dbpedia_untyped_instance SELECT resource FROM stat_resource MINUS (SELECT resource FROM dbpedia_types UNION SELECT resource FROM dbpedia_disambiguations)";
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error inserting into untyped instance table");
			e.printStackTrace();
		}
		Util.createIndex("dbpedia_untyped_instance", "resource");
		Util.checkTable("dbpedia_untyped_instance");
		
		create = "CREATE TABLE stat_resource_predicate_type (resource VARCHAR(1000), predicate VARCHAR(1000), type VARCHAR(1000), tf INT, percentage FLOAT, weight FLOAT)";
		Util.removeTableIfExisting("stat_resource_predicate_type");
		try {
			stmt.execute(create);
		} catch (SQLException e) {
			System.out.println("Error creating untyped instance table");
			e.printStackTrace();
		}
		
		insert = "INSERT INTO stat_resource_predicate_type SELECT instance.resource,tf.predicate,perc.type,tf,percentage,weight FROM dbpedia_untyped_instance as instance LEFT JOIN stat_resource_predicate_tf as tf on instance.resource = tf.resource LEFT JOIN stat_type_predicate_percentage as perc on tf.predicate = perc.predicate and tf.outin = perc.outin  LEFT JOIN stat_predicate_weight_apriori as weight on tf.predicate = weight.predicate and tf.outin = weight.outin LEFT JOIN stat_type_apriori_probability as tap on perc.type = tap.type";		
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error inserting into SDType basic stats table");
			e.printStackTrace();
		}
		Util.createIndex("stat_resource_predicate_type", "resource");
		Util.checkTable("stat_resource_predicate_type");
	}
	
	public void writeTypeFile(String fileName, float threshold) throws IOException {
		try {
			JWNL.initialize(new FileInputStream("./wordnet/file_properties.xml"));
		} catch (JWNLException e1) {
			System.out.println("Error initializing WordNet");
			e1.printStackTrace();
		}

		System.out.println("Writing types to file " + fileName);
		FileWriter FW = new FileWriter(fileName);
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error accessing sd_types table");
			e.printStackTrace();
		}
		
		String query = "SELECT resource FROM dbpedia_untyped_instance";
		try {
			ResultSet RS = stmt.executeQuery(query);
			while(RS.next()) {
				String resource = RS.getString(1);
				try {
					if(testCommonNoun(resource)) {
//						System.out.println("Skipping " + resource + " after WordNet test");
					} else {
						String resourceEscaped = resource;
						resourceEscaped = resourceEscaped.replace("'", "''");
						String query2 = "SELECT type,SUM(tf*percentage*weight)/SUM(tf*weight) AS score FROM stat_resource_predicate_type WHERE resource='" + resourceEscaped + "' GROUP BY type HAVING score>=" + threshold;
						Statement stmt2 = conn.createStatement();
						ResultSet RS2 = stmt2.executeQuery(query2);
						while(RS2.next()) {
							String type = RS2.getString(1);
							FW.write("<" + resource + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + type + ">" + System.lineSeparator());
						}
						RS2.close();
					}
				} catch (JWNLException e) {
					System.out.println("WordNet error");
					e.printStackTrace();
				}
			}
		} catch (SQLException e) {
			System.out.println("Error reading SDTypes");
			e.printStackTrace();
		}
		
		FW.close();
		
	}
	
	private String lastURI = "";
	private boolean lastResult = false;
	
	private boolean testCommonNoun(String uri) throws JWNLException {
		if(uri.equals(lastURI))
			return lastResult;
		String fragment = uri.substring(uri.lastIndexOf("/") +1);
		
		lastResult = isCommonNoun(fragment);
		lastURI = uri;
		
		return lastResult;
	}
	
	private boolean isCommonNoun(String word) throws JWNLException {
		word = word.replace("_", " ");
		if(containsNonAlpha(word))
			return false;
		IndexWord iw = Dictionary.getInstance().lookupIndexWord(POS.NOUN, word);
		if(iw==null)
			return false;
		for(Synset set : iw.getSenses()) {
			for(Word w : set.getWords()) {
				String lemma = w.getLemma();
				if(lemma.equalsIgnoreCase(word)) {
					String first1 = lemma.substring(0,1);
					if(first1.toLowerCase().equals(first1))
						return true;
				}
			}
		}
		return false;
	}
	
	private boolean containsNonAlpha(String s) {
		return !s.matches("[A-Za-z\\s]*");
	}

}
