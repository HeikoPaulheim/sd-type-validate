package de.dwslab.sdtv;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Heiko
 *
 */
public class ComputeBaseStatistics {
	public void computeGlobalTypeDistribution() {
		System.out.println("Computing global type distribution");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
			
		Util.removeTableIfExisting("stat_resource");
		String createStatement = "CREATE TABLE stat_resource (resource VARCHAR(1000) UNIQUE NOT NULL)";
		try {
			stmt = conn.createStatement();
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating global resource count table");
			e.printStackTrace();
		}
		String insertWhole = "INSERT INTO stat_resource SELECT distinct resource FROM (SELECT subject AS resource FROM dbpedia_properties UNION SELECT object AS resource FROM dbpedia_properties)";
		try {
			stmt.execute(insertWhole);
		} catch (SQLException e) {
			System.out.println("Error inserting into global resource count table");
			e.printStackTrace();
		}
		Util.checkTable("stat_resource");

		Util.removeTableIfExisting("stat_type_count");
		createStatement = "CREATE TABLE stat_type_count (type VARCHAR(1000) NOT NULL, type_count INT NOT NULL);";
		try {
			stmt = conn.createStatement();
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating global type count table");
			e.printStackTrace();
		}

		String insertStatement = "INSERT INTO stat_type_count SELECT type,COUNT(resource) FROM dbpedia_types GROUP BY (type);";
		try {
			stmt.execute(insertStatement);
		} catch (SQLException e) {
			System.out.println("Error inserting into global type count table");
			e.printStackTrace();
		}
		Util.createIndex("stat_type_count", "type");
		Util.checkTable("stat_type_count");
		
		Util.removeTableIfExisting("stat_type_apriori_probability");
		createStatement = "CREATE TABLE stat_type_apriori_probability (type VARCHAR(1000) NOT NULL, probability FLOAT NOT NULL);";
		try {
			stmt = conn.createStatement();
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating type apriori probability table");
			e.printStackTrace();
		}
		Util.createIndex("stat_type_apriori_probability", "type");

		insertStatement = "INSERT INTO stat_type_apriori_probability select type,1.0*type_count/(select count(resource) from stat_resource) AS rel_count from stat_type_count;";
		try {
			stmt.execute(insertStatement);
		} catch (SQLException e) {
			System.out.println("Error inserting into type apriori probability table");
			e.printStackTrace();
		}
		Util.checkTable("stat_type_apriori_probability");
	}
	
	public void computePerPredicateDistribution() {
		System.out.println("Computing distribution per predicate");
		Connection conn = ConnectionManager.getConnection();
		Statement stmt = null;
		
		Util.removeTableIfExisting("stat_resource_predicate_tf");
		String createStatement = "CREATE TABLE stat_resource_predicate_tf (resource VARCHAR(1000), predicate VARCHAR(1000), tf INT, outin INT)";
		try {
			stmt = conn.createStatement();
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating resource predicate count table");
			e.printStackTrace();
		}
		String insert1 = "INSERT INTO stat_resource_predicate_tf SELECT subject, predicate, COUNT(object),0 FROM dbpedia_properties GROUP BY subject, predicate;";
		String insert2 = "INSERT INTO stat_resource_predicate_tf SELECT object, predicate, COUNT(subject),1 FROM dbpedia_properties GROUP BY object, predicate;";
		try {
			stmt.execute(insert1);
			stmt.execute(insert2);
		} catch (SQLException e) {
			System.out.println("Error inserting into resource predicate count table");
			e.printStackTrace();
		}
		Util.createIndex("stat_resource_predicate_tf", "resource");
		Util.createIndex("stat_resource_predicate_tf", "predicate");
		Util.checkTable("stat_resource_predicate_tf");
		
		Util.removeTableIfExisting("stat_type_predicate_percentage");
		createStatement = "CREATE TABLE stat_type_predicate_percentage (type VARCHAR(1000), predicate VARCHAR(1000), outin INT, percentage FLOAT)";
		try {
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating type predicate distribution table");
			e.printStackTrace();
		}
		insert1 = "INSERT INTO stat_type_predicate_percentage SELECT types.type, res.predicate, 0, 1.0*COUNT(subject)/(SELECT COUNT(subject) FROM dbpedia_properties AS resinner WHERE res.predicate = resinner.predicate) FROM dbpedia_properties AS res, dbpedia_types AS types WHERE res.subject = types.resource GROUP BY res.predicate,types.type";	
		insert2 = "INSERT INTO stat_type_predicate_percentage SELECT types.type, res.predicate, 1, 1.0*COUNT(object)/(SELECT COUNT(object) FROM dbpedia_properties AS resinner WHERE res.predicate = resinner.predicate) FROM dbpedia_properties AS res, dbpedia_types AS types WHERE res.object = types.resource GROUP BY res.predicate,types.type;";
		try {
			stmt.execute(insert1);
			stmt.execute(insert2);
		} catch (SQLException e) {
			System.out.println("Error inserting into type predicate distribution table");
			e.printStackTrace();
		}
		Util.createIndex("stat_type_predicate_percentage", "type");
		Util.createIndex("stat_type_predicate_percentage", "predicate");
		Util.checkTable("stat_type_predicate_percentage");

		Util.removeTableIfExisting("stat_predicate_weight_apriori");
		createStatement = "CREATE TABLE stat_predicate_weight_apriori (predicate VARCHAR(1000), outin INT, weight FLOAT)";
		try {
			stmt.execute(createStatement);
		} catch (SQLException e) {
			System.out.println("Error creating predicate weight table");
			e.printStackTrace();
		}
		
		String insert = "INSERT INTO stat_predicate_weight_apriori SELECT predicate,outin,SUM((percentage - probability)*(percentage - probability)) FROM stat_type_predicate_percentage LEFT JOIN stat_type_apriori_probability ON stat_type_predicate_percentage.type = stat_type_apriori_probability.type GROUP BY predicate,outin";
		try {
			stmt.execute(insert);
		} catch (SQLException e) {
			System.out.println("Error inserting into predicate weight table");
			e.printStackTrace();
		}
		Util.createIndex("stat_predicate_weight_apriori", "predicate");
		Util.checkTable("stat_predicate_weight_apriori");
	}
}
