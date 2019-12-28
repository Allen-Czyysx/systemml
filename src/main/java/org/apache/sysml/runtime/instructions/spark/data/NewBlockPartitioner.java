package org.apache.sysml.runtime.instructions.spark.data;

import org.apache.sysml.runtime.matrix.MatrixCharacteristics;

public class NewBlockPartitioner extends BlockPartitioner {

	private static final long serialVersionUID = 3207938407732880324L;

	public NewBlockPartitioner(MatrixCharacteristics mc, int numParts) {
		super(mc, numParts);
	}

	@Override
	public int getPartition(Object arg0) {
		return hash(super.getPartition(arg0)) % _numParts;
	}

	private static int hash(int h) {
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

}
