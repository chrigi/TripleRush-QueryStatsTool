package ch.ba.qcost.cost;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class TraceCostContainer {
	
	private int[] intraEdges;
	private int[] interEdges;
	private int[][] queryDistribution;
	private int[][] sigIdDistribution;
	
	private int[][][] workerDistributionDist;
	private int[][][] workerDistributionAlt;
	
	private int[][][][] workerMessagesDist;
	private int[][][][] workerMessagesAlt;
	
	private int noQueries;
	private int noNodes;
	
	public TraceCostContainer(int noQueries, int noNodes) {
		this.noQueries = noQueries;
		this.noNodes = noNodes;
		intraEdges = new int[noQueries];
		interEdges = new int[noQueries];
		queryDistribution = new int[noQueries][noNodes];
		sigIdDistribution = new int[noQueries][noNodes];
		workerDistributionDist = new int[noQueries][noNodes][23];
		workerDistributionAlt = new int[noQueries][noNodes][23];
		workerMessagesDist = new int[noQueries][noNodes][23][3];
		workerMessagesAlt = new int[noQueries][noNodes][23][3];
	}
	
	public void incrementIntraEdges(int queryId) {
		intraEdges[queryId]++;
	}
	
	public void incrementInterEdges(int queryId) {
		interEdges[queryId]++;
	}
	
	public void incrementQueryDistribution(int queryId, int node) {
		queryDistribution[queryId][node]++;
	}
	
	public void incrementSigIdDistribution(int queryId, int node) {
		sigIdDistribution[queryId][node]++;
	}
	
	public void incrementWorkerDistributionDist(int queryId, int node, int worker) {
		workerDistributionDist[queryId][node][worker]++;
	}
	
	public void incrementWorkerDistributionAlt(int queryId, int node, int worker) {
		workerDistributionAlt[queryId][node][worker]++;
	}
	
	public void incrementWorkerMessagesDist(int queryId, int node, int worker, int destination) {
		workerMessagesDist[queryId][node][worker][destination]++;
	}
	
	public void incrementWorkerMessagesAlt(int queryId, int node, int worker, int destination) {
		workerMessagesAlt[queryId][node][worker][destination]++;
	}
	
	public void writeToFiles(String outPath, String dataset, int noNodes, String desc) {
		
		System.out.println("Writing Costs to File...");
		
		File edgesFile = new File(outPath + "stats_" + desc + "_edges_" + dataset + "_" + noNodes);
		File vertDistFile = new File(outPath + "stats_" + desc + "_vertDist_" + dataset + "_" + noNodes);
		File workDistFile = new File(outPath + "stats_" + desc + "_workDist_" + dataset + "_" + noNodes);
		File workMsgFile = new File(outPath + "stats_" + desc + "_workMsg_" + dataset + "_" + noNodes);

		BufferedWriter writer = null;
		try {
			if (edgesFile.exists()) {
				edgesFile.delete();
			}
			edgesFile.createNewFile();

			writer = new BufferedWriter(new FileWriter(edgesFile));
			
			writer.write("---");
			for(int i = 0; i < noQueries; ++i) {
				writer.write(";Query " + (i+1));
			}
			writer.newLine();
			
			writer.write("Inter Edges");
			for(int i = 0; i < noQueries; ++i) {
				writer.write(";" + interEdges[i]);
			}
			writer.newLine();
			
			writer.write("Intra Edges");
			for(int i = 0; i < noQueries; ++i) {
				writer.write(";" + intraEdges[i]);
			}
			writer.newLine();
			
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
			
			//---------------------
			
			if (vertDistFile.exists()) {
				vertDistFile.delete();
			}
			vertDistFile.createNewFile();

			writer = new BufferedWriter(new FileWriter(vertDistFile));
			
			writer.write("-Vert-");
			for(int i = 0; i < noNodes; ++i) {
				writer.write(";Node " + i);
			}
			writer.newLine();
			
			for(int j = 0; j < noQueries; ++j) {
				writer.write("Query " + (j+1));
				for(int i = 0; i < noNodes; ++i) {
					writer.write(";" + queryDistribution[j][i]);
				}
				writer.newLine();
			}
			
			writer.newLine();
			
			writer.write("-SigId-");
			for(int i = 0; i < noNodes; ++i) {
				writer.write(";Node " + i);
			}
			writer.newLine();
			
			for(int j = 0; j < noQueries; ++j) {
				writer.write("Query " + (j+1));
				for(int i = 0; i < noNodes; ++i) {
					writer.write(";" + sigIdDistribution[j][i]);
				}
				writer.newLine();
			}
			
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
			
			//---------------------
			
			if (workDistFile.exists()) {
				workDistFile.delete();
			}
			workDistFile.createNewFile();

			writer = new BufferedWriter(new FileWriter(workDistFile));
				
			writer.write("-Dist-");
			for(int i = 0; i < (noNodes*23); ++i) {
				writer.write(";Worker " + ((100*(i/23) + i%23) + 1));
			}
			writer.newLine();
			
			for(int j = 0; j < noQueries; ++j) {
				writer.write("Query " + (j+1));
				for(int i = 0; i < (noNodes*23); ++i) {
					writer.write(";" + workerDistributionDist[j][(i/23)][(i%23)]);
				}
				writer.newLine();
			}
			
			writer.newLine();

			writer.write("-Alt-");
			for(int i = 0; i < (noNodes*23); ++i) {
				writer.write(";Worker " + ((100*(i/23) + i%23) + 1));
			}
			writer.newLine();
			
			for(int j = 0; j < noQueries; ++j) {
				writer.write("Query " + (j+1));
				for(int i = 0; i < (noNodes*23); ++i) {
					writer.write(";" + workerDistributionAlt[j][(i/23)][(i%23)]);
				}
				writer.newLine();
			}
			
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
			
			//---------------------
			
			if (workMsgFile.exists()) {
				workMsgFile.delete();
			}
			workMsgFile.createNewFile();

			writer = new BufferedWriter(new FileWriter(workMsgFile));
			
			for(int j = 0; j < noQueries; ++j) {
				
				writer.write("-Dist Query " + (j+1) + "-");
				for(int i = 0; i < (noNodes*23); ++i) {
					writer.write(";Worker " + ((100*(i/23) + i%23) + 1));
				}
				writer.newLine();
				
				for(int k = 0; k < 3; ++k) {
					writer.write("Msg Type " + k);
					for(int i = 0; i < (noNodes*23); ++i) {
						writer.write(";" + workerMessagesDist[j][(i/23)][(i%23)][k]);
					}
					writer.newLine();
				}
				
				writer.newLine();
			}
			
			for(int j = 0; j < noQueries; ++j) {
				
				writer.write("-Alt Query " + (j+1) + "-");
				for(int i = 0; i < (noNodes*23); ++i) {
					writer.write(";Worker " + ((100*(i/23) + i%23) + 1));
				}
				writer.newLine();
				
				for(int k = 0; k < 3; ++k) {
					writer.write("Msg Type " + k);
					for(int i = 0; i < (noNodes*23); ++i) {
						writer.write(";" + workerMessagesAlt[j][(i/23)][(i%23)][k]);
					}
					writer.newLine();
				}
				
				writer.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
		}
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(interEdges);
		result = prime * result + Arrays.hashCode(intraEdges);
		result = prime * result + noNodes;
		result = prime * result + noQueries;
		result = prime * result + Arrays.hashCode(queryDistribution);
		result = prime * result + Arrays.hashCode(workerDistributionAlt);
		result = prime * result + Arrays.hashCode(workerDistributionDist);
		result = prime * result + Arrays.hashCode(workerMessagesAlt);
		result = prime * result + Arrays.hashCode(workerMessagesDist);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TraceCostContainer other = (TraceCostContainer) obj;
		if (!Arrays.equals(interEdges, other.interEdges))
			return false;
		if (!Arrays.equals(intraEdges, other.intraEdges))
			return false;
		if (noNodes != other.noNodes)
			return false;
		if (noQueries != other.noQueries)
			return false;
		if (!Arrays.deepEquals(queryDistribution, other.queryDistribution))
			return false;
		if (!Arrays.deepEquals(workerDistributionAlt, other.workerDistributionAlt))
			return false;
		if (!Arrays.deepEquals(workerDistributionDist, other.workerDistributionDist))
			return false;
		if (!Arrays.deepEquals(workerMessagesAlt, other.workerMessagesAlt))
			return false;
		if (!Arrays.deepEquals(workerMessagesDist, other.workerMessagesDist))
			return false;
		return true;
	}

}
