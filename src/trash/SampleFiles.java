package trash;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class SampleFiles {
	
	private static int sampleSize = 10000;
	private static String typeIn = "C:\\Users\\Heiko\\Documents\\Forschung\\DBpediaBucketing\\enwiki-20151002-instance-types-transitive.ttl";
	private static String typeOut = "C:\\Users\\Heiko\\Documents\\Forschung\\DBpediaBucketing\\enwiki-20151002-instance-types-transitive-sample.ttl";
	private static String propIn = "C:\\Users\\Heiko\\Documents\\Forschung\\DBpediaBucketing\\enwiki-20151002-mappingbased-objects-uncleaned.ttl";
	private static String propOut = "C:\\Users\\Heiko\\Documents\\Forschung\\DBpediaBucketing\\enwiki-20151002-mappingbased-objects-uncleaned-sample.ttl";

	private static Set<String> sampleInstances = new HashSet<String>();
	
	public static void main(String[] args) throws IOException {

		// sample the first 1000 instances appearing in any statement
		BufferedReader BR = new BufferedReader(new FileReader(propIn));
		while(BR.ready()) {
			String line = BR.readLine();
			if(line.startsWith("#"))
				continue;
			line=line.replace("'","''");
			StringTokenizer stk = new StringTokenizer(line,"> ",false);
			String subject = stk.nextToken();
			subject = subject.replace("<","");
			sampleInstances.add(subject);
			
			String object = stk.nextToken();
			if(object.startsWith("<")) {
				if(object.startsWith("<http://dbpedia.org/resource/")) {
					object = object.replace(" .","");
					object = object.replace("<","");
					sampleInstances.add(object);
				}
			}
			if(sampleInstances.size()>=sampleSize)
				break;
		}
		BR.close();

		// create sample from prop File
		BR = new BufferedReader(new FileReader(propIn));
		FileWriter FW = new FileWriter(propOut);
		while(BR.ready()) {
			String line = BR.readLine();
			if(line.startsWith("#"))
				continue;
			line=line.replace("'","''");
			StringTokenizer stk = new StringTokenizer(line,"> ",false);
			String subject = stk.nextToken();
			subject = subject.replace("<","");
			
			String object = stk.nextToken();
			if(object.startsWith("<")) {
				if(object.startsWith("<http://dbpedia.org/resource/")) {
					object = object.replace(" .","");
					object = object.replace("<","");
				}
			}
			
			if(sampleInstances.contains(subject)||sampleInstances.contains(object))
				FW.write(line + System.lineSeparator());
		}
		BR.close();
		FW.close();

		BR = new BufferedReader(new FileReader(typeIn));
		FW = new FileWriter(typeOut);
		while(BR.ready()) {
			String line = BR.readLine();
			if(line.startsWith("#"))
				continue;
			line=line.replace("'","''");
			StringTokenizer stk = new StringTokenizer(line,"> ",false);
			String subject = stk.nextToken();
			subject = subject.replace("<","");

			if(sampleInstances.contains(subject))
				FW.write(line + System.lineSeparator());
		}
		BR.close();
		FW.close();
	}
}
