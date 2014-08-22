package ch.ba.qcost;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import ch.ba.qcost.cost.TraceCostContainer;
import ch.ba.qcost.dict.DictCostCalculator;
import ch.ba.qcost.table.TableCostCalculator;

public class TraceCost {

	public static void main(String[] args) {

		System.out.println("=== Trace Cost Calculator ===");

		// --- Load Parameters -----------------------------------------------------------------------------------------------
		
		final Properties params = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream(args[0]);
			params.load(input);
			System.out.println("Launch Parameters:");
			System.out.println(params.toString());
		} catch (FileNotFoundException e) {
	        e.printStackTrace();
	        System.exit(0);
	    }catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}
		}

		String dataset = params.getProperty("DATASET");

		int noNodes = Integer.parseInt(params.getProperty("NO_NODES"));
		
		int queryIdMin = Integer.parseInt(params.getProperty("QUERY_ID_MIN"));
		int noQueries = Integer.parseInt(params.getProperty("NO_QUERIES"));
		
		String outPath = params.getProperty("OUT_PATH");
		
		String traceFilesPath = params.getProperty("TRACES_PATH");
		String tableFilesPath = params.getProperty("TABLES_PATH");
		String idMapPath = params.getProperty("IDMAP_PATH");
		
		// --- Cost ----------------------------------------------------------------------------------------------------------
		
		System.out.println("Calculating Cost without Dictionary or Lookup Table");
		TraceCostContainer bdCost = DictCostCalculator.calculateDictCost(traceFilesPath, dataset, noNodes, false, queryIdMin, noQueries, idMapPath);
		bdCost.writeToFiles(outPath, dataset, noNodes, "before");
		
		// --- Dictionary Cost -----------------------------------------------------------------------------------------------
		
		System.out.println("= Calculating Cost for Dictionary Method =");
		
		System.out.println("Calculating Cost with Dictionary");
		TraceCostContainer adCost = DictCostCalculator.calculateDictCost(traceFilesPath, dataset, noNodes, true, queryIdMin, noQueries, idMapPath);
		adCost.writeToFiles(outPath, dataset, noNodes, "dict");
		
		// --- Lookup Table Cost ---------------------------------------------------------------------------------------------
		
		System.out.println("= Calculating Cost for Lookup Table Method =");
		
		System.out.println("Calculating Cost with Lookup Table");
		TraceCostContainer atCost = TableCostCalculator.calculateTableCost(traceFilesPath, tableFilesPath, dataset, noNodes, queryIdMin, noQueries);
		atCost.writeToFiles(outPath, dataset, noNodes, "table");
	}
}
