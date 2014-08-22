package ch.ba.qcost.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ch.ba.qcost.cost.TraceCostContainer;
import ch.ba.qcost.util.TPProcessor;

public class TableCostCalculator {

	public static TraceCostContainer calculateTableCost(String traceFilesPath, String tableFilesPath, String dataset, int noNodes, int queryIdMin, int noQueries) {
		
		Map<String, Integer> lookupTable = TableCostCalculator.generateLookupTable(tableFilesPath, dataset, noNodes);
		
		System.out.println("Processing Query Trace Files...");
		
		TraceCostContainer tCost = new TraceCostContainer(noQueries, noNodes);
		List<Set<String>> processedVertices = new ArrayList<Set<String>>(noQueries);
		List<Set<String>> processedEdges = new ArrayList<Set<String>>(noQueries);
		List<Set<Integer>> processedSigId = new ArrayList<Set<Integer>>(noQueries);
		
		for (int i = 0; i < noQueries; ++i) {
			processedVertices.add(new HashSet<String>());
			processedEdges.add(new HashSet<String>());
			processedSigId.add(new HashSet<Integer>());
		}
		
		File tracesFolder = new File(traceFilesPath + dataset + "/" + noNodes + "_nodes/");
		System.out.println("\tTraces folder: " + tracesFolder.getAbsolutePath());
		for (File traceFile : tracesFolder.listFiles()) {

			System.out.println("\tReading File: " + traceFile.getName());

			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(traceFile));

				String line = "";
				while ((line = in.readLine()) != null) {

					// Trace format: queryId sourceTP destinationTP queryType
					// queryType = forwarding/redirecting
					String[] trace = line.split(" ");
					int query = Integer.parseInt(trace[0]);
					String source = trace[1];
					String dest = trace[2];
					int zero_query = query-queryIdMin;
					
					Integer source_node = lookupTable.get(source);
					if (source_node == null) {
						source_node = TPProcessor.getNodeNumber(source, noNodes);
					}
					
					Integer dest_node = lookupTable.get(dest);
					if (dest_node == null) {
						dest_node = TPProcessor.getNodeNumber(dest, noNodes);
					}
					
					int source_worker_dist = (TPProcessor.getSigId(trace[1]) + TPProcessor.getSecondId(trace[1])) % 23;
					int dest_worker_dist = (TPProcessor.getSigId(trace[2]) + TPProcessor.getSecondId(trace[2])) % 23;
					
					int source_worker_alt = TPProcessor.getSigId(trace[1]) % 23;
					int dest_worker_alt = TPProcessor.getSigId(trace[2]) % 23;

					tCost.incrementWorkerDistributionDist(zero_query, source_node, source_worker_dist);
//					tCost.incrementWorkerDistributionDist(zero_query, dest_node, dest_worker_dist);
					
					tCost.incrementWorkerDistributionAlt(zero_query, source_node, source_worker_alt);
//					tCost.incrementWorkerDistributionAlt(zero_query, dest_node, dest_worker_alt);
					
					if (source_node.intValue() != dest_node.intValue()) {
						if (!processedEdges.get(zero_query).contains((trace[1]+trace[2]))) {
							tCost.incrementInterEdges(zero_query);
							processedEdges.get(zero_query).add((trace[1]+trace[2]));
						}
						tCost.incrementWorkerMessagesDist(zero_query, source_node.intValue(), source_worker_dist, 2);
						tCost.incrementWorkerMessagesAlt(zero_query, source_node.intValue(), source_worker_alt, 2);
					} else {
						if (!processedEdges.get(zero_query).contains((trace[1]+trace[2]))) {
							tCost.incrementIntraEdges(zero_query);
							processedEdges.get(zero_query).add((trace[1]+trace[2]));
						}
						if (source_worker_dist == dest_worker_dist) {
							tCost.incrementWorkerMessagesDist(zero_query, source_node.intValue(), source_worker_dist, 0);
						} else {
							tCost.incrementWorkerMessagesDist(zero_query, source_node.intValue(), source_worker_dist, 1);
						}
						if (source_worker_alt == dest_worker_alt) {
							tCost.incrementWorkerMessagesAlt(zero_query, source_node.intValue(), source_worker_alt, 0);
						} else {
							tCost.incrementWorkerMessagesAlt(zero_query, source_node.intValue(), source_worker_alt, 1);
						}
					}
					
					if (!processedVertices.get(zero_query).contains(trace[1])) {
						tCost.incrementQueryDistribution(zero_query, source_node.intValue());
						processedVertices.get(zero_query).add(trace[1]);
					}
					if (!processedVertices.get(zero_query).contains(trace[2])) {
						tCost.incrementQueryDistribution(zero_query, dest_node.intValue());
						processedVertices.get(zero_query).add(trace[2]);
					}
					if ((!processedSigId.get(zero_query).contains(TPProcessor.getSigId(trace[1])))) {
						tCost.incrementSigIdDistribution(zero_query, source_node);
						processedSigId.get(zero_query).add(TPProcessor.getSigId(trace[1]));
					}
					if ((!processedSigId.get(zero_query).contains(TPProcessor.getSigId(trace[2])))) {
						tCost.incrementSigIdDistribution(zero_query, dest_node);
						processedSigId.get(zero_query).add(TPProcessor.getSigId(trace[2]));
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}

		}

		return tCost;
	}
	
	private static Map<String, Integer> generateLookupTable(String tableFilesPath, String dataset, int noNodes) {
		
		System.out.println("Processing Lookup Table File...");
		Map<String, Integer> table = new HashMap<String, Integer>();
		
		File tableFile = new File(tableFilesPath + "qt-table_" + dataset + "_" + noNodes);
		System.out.println("\tTable file: " + tableFile.getAbsolutePath());
		
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(tableFile));

			String line = "";
			while ((line = in.readLine()) != null) {

				String[] entry = line.split(" -> ");
				String tp = entry[0];
				int node = Integer.parseInt(entry[1]);
				
				table.put(tp, node);
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		return table;
	}
}
