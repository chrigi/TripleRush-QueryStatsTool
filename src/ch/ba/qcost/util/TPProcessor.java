package ch.ba.qcost.util;

public class TPProcessor {

	/**
	 * Extract IDs from a TriplePattern.
	 * 
	 * @param tp
	 *            TriplePattern
	 * @return Array of subject, predicate, object IDs
	 */
	public static int[] extractIds(String tp) {
		String[] tp_ids = tp.substring(tp.indexOf('(') + 1, tp.lastIndexOf(')')).split(",");
		return new int[] { Integer.parseInt(tp_ids[0]), Integer.parseInt(tp_ids[1]), Integer.parseInt(tp_ids[2]) };
	}

	/**
	 * Extract significant ID from TriplePattern IDs.
	 * <p>
	 * Hierarchy: Subject, Object, Predicate
	 * 
	 * @param ids
	 *            Array of subject, predicate, object IDs
	 * @return Significant ID
	 */
	public static int getSigId(int[] ids) {
		if (ids[0] != 0) {
			return ids[0];
		} else if (ids[2] != 0) {
			return ids[2];
		} else {
			return ids[1];
		}
	}
	
	/**
	 * Extract second ID from TriplePattern IDs.
	 * 
	 * @param ids
	 *            Array of subject, predicate, object IDs
	 * @param firstId
	 * 			  Significant ID from TriplePattern IDs
	 * @return Significant ID
	 */
	public static int getSecondId(int[] ids, int firstId) {
		int secondId = 0;
		
		for (int i : ids) {
			if ((i != firstId) && (i != 0)) {
				secondId = i;
			}
		}
		
		return secondId;
	}

	/**
	 * Extract significant ID from a TriplePattern.
	 * <p>
	 * Hierarchy: Subject, Object, Predicate
	 * 
	 * @param tp
	 *            TriplePattern
	 * @return Significant ID
	 */
	public static int getSigId(String tp) {
		return TPProcessor.getSigId(TPProcessor.extractIds(tp));
	}
	
	/**
	 * Extract second ID from a TriplePattern.
	 * 
	 * @param tp
	 *            TriplePattern
	 * @return Significant ID
	 */
	public static int getSecondId(String tp) {
		int[] ids = TPProcessor.extractIds(tp);
		int firstId = TPProcessor.getSigId(ids);
		return TPProcessor.getSecondId(ids, firstId);
	}

	/**
	 * Extract natural node number from TriplePattern.
	 * 
	 * @param tp
	 *            TriplePattern
	 * @param noNodes
	 *            Number of Nodes over which the graph will be distributed
	 * @return Natural node number on which the TriplePattern would be placed
	 */
	public static int getNodeNumber(String tp, int noNodes) {
		return TPProcessor.getSigId(tp) % noNodes;
	}
}
