package ch.ba.qcost.dict;

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

public class DictCostCalculator {

	public static TraceCostContainer calculateDictCost(String traceFilesPath, String dataset, int noNodes, boolean withDict, int queryIdMin, int noQueries, String idMapFilePath) {
		
		System.out.println("Processing Query Trace Files...");
		
		TraceCostContainer dCost = new TraceCostContainer(noQueries, noNodes);
		File tracesFolder = null;
		List<Set<String>> processedVertices = new ArrayList<Set<String>>(noQueries);
		List<Set<String>> processedEdges = new ArrayList<Set<String>>(noQueries);
		List<Set<Integer>> processedSigId = new ArrayList<Set<Integer>>(noQueries);
		
		for (int i = 0; i < noQueries; ++i) {
			processedVertices.add(new HashSet<String>());
			processedEdges.add(new HashSet<String>());
			processedSigId.add(new HashSet<Integer>());
		}
		
		Map<Integer, Integer> idMap = null;
		
		tracesFolder = new File(traceFilesPath + dataset + "/" + noNodes + "_nodes/");
		
		if (withDict) {	
			idMap = generateIdMapTable(idMapFilePath, dataset, noNodes);
		}
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
					int source_node = TPProcessor.getNodeNumber(trace[1], noNodes);
					int dest_node = TPProcessor.getNodeNumber(trace[2], noNodes);
					int zero_query = query-queryIdMin;
					
					int source_worker_dist = (TPProcessor.getSigId(trace[1]) + TPProcessor.getSecondId(trace[1])) % 23;
					int dest_worker_dist = (TPProcessor.getSigId(trace[2]) + TPProcessor.getSecondId(trace[2])) % 23;
					
					int source_worker_alt = TPProcessor.getSigId(trace[1]) % 23;
					int dest_worker_alt = TPProcessor.getSigId(trace[2]) % 23;
					
					if(withDict) {
						source_node = idMap.get(TPProcessor.getSigId(trace[1])) % noNodes;
						dest_node = idMap.get(TPProcessor.getSigId(trace[2])) % noNodes;
					}
					
					dCost.incrementWorkerDistributionDist(zero_query, source_node, source_worker_dist);
//					dCost.incrementWorkerDistributionDist(zero_query, dest_node, dest_worker_dist);
					
					dCost.incrementWorkerDistributionAlt(zero_query, source_node, source_worker_alt);
//					dCost.incrementWorkerDistributionAlt(zero_query, dest_node, dest_worker_alt);
					
					if (source_node != dest_node) {
						if (!processedEdges.get(zero_query).contains((trace[1]+trace[2]))) {
							dCost.incrementInterEdges(zero_query);
							processedEdges.get(zero_query).add((trace[1]+trace[2]));
						}
						dCost.incrementWorkerMessagesDist(zero_query, source_node, source_worker_dist, 2);
						dCost.incrementWorkerMessagesAlt(zero_query, source_node, source_worker_alt, 2);
					} else {
						if (!processedEdges.get(zero_query).contains((trace[1]+trace[2]))) {
							dCost.incrementIntraEdges(zero_query);
							processedEdges.get(zero_query).add((trace[1]+trace[2]));
						}
						if (source_worker_dist == dest_worker_dist) {
							dCost.incrementWorkerMessagesDist(zero_query, source_node, source_worker_dist, 0);
						} else {
							dCost.incrementWorkerMessagesDist(zero_query, source_node, source_worker_dist, 1);
						}
						if (source_worker_alt == dest_worker_alt) {
							dCost.incrementWorkerMessagesAlt(zero_query, source_node, source_worker_alt, 0);
						} else {
							dCost.incrementWorkerMessagesAlt(zero_query, source_node, source_worker_alt, 1);
						}
					}
					
					if (!processedVertices.get(zero_query).contains(trace[1])) {
						dCost.incrementQueryDistribution(zero_query, source_node);
						processedVertices.get(zero_query).add(trace[1]);
					}
					if ((!processedVertices.get(zero_query).contains(trace[2]))) {
						dCost.incrementQueryDistribution(zero_query, dest_node);
						processedVertices.get(zero_query).add(trace[2]);
					}
					if ((!processedSigId.get(zero_query).contains(TPProcessor.getSigId(trace[1])))) {
						dCost.incrementSigIdDistribution(zero_query, source_node);
						processedSigId.get(zero_query).add(TPProcessor.getSigId(trace[1]));
					}
					if ((!processedSigId.get(zero_query).contains(TPProcessor.getSigId(trace[2])))) {
						dCost.incrementSigIdDistribution(zero_query, dest_node);
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

		return dCost;
	}
	
private static Map<Integer, Integer> generateIdMapTable(String mapFilePath, String dataset, int noNodes) {
		
		System.out.println("Processing Lookup Table File...");
		Map<Integer, Integer> table = new HashMap<Integer, Integer>();
		
		File tableFile = new File(mapFilePath + "qt-idMap_" + dataset + "_" + noNodes);
		System.out.println("\tidMap file: " + tableFile.getAbsolutePath());
		
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(tableFile));

			String line = "";
			while ((line = in.readLine()) != null) {

				String[] entry = line.split(" -> ");
				int oldId = Integer.parseInt(entry[0]);
				int newId = Integer.parseInt(entry[1]);
				
				table.put(oldId, newId);
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
