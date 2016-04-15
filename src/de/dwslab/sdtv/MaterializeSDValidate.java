package de.dwslab.sdtv;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class MaterializeSDValidate {
	private int minInDegree = 20;
	private int bulkSize = 1000;
	
	public void computeSDValidateScores() {
		System.out.println("Computing SDValidate scores");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error preparing SDValidate");
			e.printStackTrace();
		}
		
		Util.removeTableIfExisting("stat_predicate_gini");
		String create = "CREATE TABLE stat_predicate_gini (predicate VARCHAR(1000),outin INT, gini FLOAT)";
		try {
			stmt.execute(create);
		} catch (SQLException e) {
			System.out.println("Error creating predicate gini table");
			e.printStackTrace();
		}
		String insert = "INSERT INTO stat_predicate_gini SELECT predicate,outin,SUM(percentage*percentage) FROM stat_type_predicate_percentage GROUP BY predicate,outin";
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error inserting into predicate gini table");
			e.printStackTrace();
		}
		Util.checkTable("stat_predicate_gini");
		
		Util.removeTableIfExisting("sdv_interesting_resource");
		create = "CREATE TABLE sdv_interesting_resource (resource VARCHAR(1000), indegree INT)";
		try {
			stmt.execute(create);
		} catch (SQLException e) {
			System.out.println("Error creating interesting resources table");
			e.printStackTrace();
		}
		insert = "INSERT INTO sdv_interesting_resource SELECT tf.resource, sum(tf) FROM stat_resource_predicate_tf AS tf WHERE outin=1 GROUP BY tf.resource HAVING(SUM(tf)>" + (minInDegree-1) + ")";
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error insert into interesting resources table");
			e.printStackTrace();
		}
		Util.checkTable("sdv_interesting_resource");
		
		// calibrate mingini
		float minGini = 0.0f;
		try {
			String select = "SELECT gini FROM stat_predicate_gini WHERE predicate='http://www.w3.org/2004/02/skos/core#subject' AND outin=1";
			ResultSet RS = stmt.executeQuery(select);
			if(RS.next()) {
				System.out.println("skos-core:subject " + RS.getFloat(1));
				minGini = RS.getFloat(1);
			}
			select = "SELECT gini FROM stat_predicate_gini WHERE predicate='http://dbpedia.org/ontology/knownFor' AND outin=1";
			RS = stmt.executeQuery(select);
			if(RS.next()) {
				System.out.println("dbo:knownFor " + RS.getFloat(1));
				minGini = Math.max(minGini,RS.getFloat(1));
			}
			select = "SELECT gini FROM stat_predicate_gini WHERE predicate='http://www.w3.org/2000/01/rdf-schema#seeAlso' AND outin=1";
			RS = stmt.executeQuery(select);
			if(RS.next()) {
				System.out.println("rdfs:seeAlso " + RS.getFloat(1));
				minGini = Math.max(minGini,RS.getFloat(1));
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("Gini index threshold chosen: " + minGini);
		
		Util.removeTableIfExisting("sdv_statements_to_check");
		create = "CREATE TABLE sdv_statements_to_check (subject VARCHAR(1000), predicate VARCHAR(1000), object VARCHAR(1000))";
		try {
			stmt.execute(create);
		} catch (SQLException e) {
			System.out.println("Error creating statements to check table");
			e.printStackTrace();
		}
		insert = "INSERT INTO sdv_statements_to_check SELECT subject,stat.predicate,sdv.resource AS object FROM sdv_interesting_resource as sdv, stat_resource_predicate_tf AS stat, stat_predicate_gini AS gini, dbpedia_properties AS prop WHERE sdv.resource=stat.resource AND sdv.resource=prop.object AND stat.predicate=prop.predicate AND stat.outin=1 AND stat.predicate=gini.predicate AND gini.outin=1 AND gini.gini>" + minGini + " AND stat.tf/indegree<=" + (1.0f/minInDegree);
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error insert into statements to check table");
			e.printStackTrace();
		}
		Util.checkTable("sdv_statements_to_check");
	}
	
	public void writeWrongStatementsFile(String fileName, float threshold) throws IOException {
		System.out.println("Writing SD invalid statements to file " + fileName);
		FileWriter FW = new FileWriter(fileName);
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error accessing sd_validate table");
			e.printStackTrace();
		}
		
		int offset = 0;
		int i=0;
		try {
			while(true) {
				int round = 0;
				String query = "SELECT * FROM sdv_statements_to_check LIMIT " + bulkSize + " OFFSET " + offset*bulkSize;
				offset++;
			
				ResultSet RS = stmt.executeQuery(query);
				
				while(RS.next()) {
					Map<String,Double> actualTypes = getActualTypes(RS.getString(3));
					Map<String,Double> predicateTypes = getPredictedTypes(RS.getString(2), 0.05);
					double rating = getCosine(actualTypes, predicateTypes);
					if(rating<=threshold)
						FW.write("<" + RS.getString(1) + "> <" + RS.getString(2) + "> <" + RS.getString(3)  + "> ."+ System.lineSeparator());
					
					i++;
					round++;
					if(i%100==0)
						System.out.println(i + " statements done");
				}
				if(round==0)
					break;
			}
		} catch (SQLException e) {
			System.out.println("error materializing SDValidated statements");
			e.printStackTrace();
		}
		
		FW.flush();
		FW.close();
		
		System.out.println("done");
	}

	private static Map<String,Double> getActualTypes(String resource) throws SQLException {
		String resourceEscaped = resource;
		resourceEscaped = resourceEscaped.replaceAll("'", "''");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error accessing sd_validate table");
			e.printStackTrace();
		}
		String query = "SELECT type FROM dbpedia_types WHERE resource = '" + resourceEscaped + "'";
		ResultSet RS = stmt.executeQuery(query);
		
		Map<String,Double> types = new HashMap<String,Double>() ;
		
		while(RS.next()) {
			types.put(RS.getString(1),1.0);
		}
		
		return types;
	}

	private static Map<String,Double> getPredictedTypes(String predicate, double threshold) throws SQLException {
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			System.out.println("Error accessing sd_validate table");
			e.printStackTrace();
		}
		ResultSet RS = stmt.executeQuery(
		"SELECT type,percentage FROM stat_type_predicate_percentage AS pred " +
		"WHERE predicate = '" + predicate + "' " +
		"AND percentage>" + threshold);

		Map<String,Double> types = new HashMap<String,Double>() ;
				
		while(RS.next()) {
			types.put(RS.getString(1),RS.getDouble(2));
		}
		
		return types;
	}

	private static double getCosine(Map<String,Double> v1, Map<String,Double> v2) {
		if(v1.size()==0 || v2.size()==0)
			return -1.0;
		
		double num = 0.0;
		double denom1 = 0.0;
		double denom2 = 0.0;
		
		for(Map.Entry<String,Double> entry : v1.entrySet()) {
			if(v2.containsKey(entry.getKey()))
				num += entry.getValue() * v2.get(entry.getKey());
			
			denom1 += entry.getValue() * entry.getValue();
		}
		
		for(Map.Entry<String,Double> entry : v2.entrySet()) {
			denom2 += entry.getValue() * entry.getValue();
		}
			
		return num / (Math.sqrt(denom1) * Math.sqrt(denom2)); 
	}
	
}
