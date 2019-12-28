package org.apache.sysml.runtime.instructions.spark.data;

import org.apache.spark.Partitioner;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class CpmmIndexPartitioner extends Partitioner {

	private static final long serialVersionUID = 3207938407732880324L;

	private ColPartitioner _partitioner;

	public CpmmIndexPartitioner(MatrixCharacteristics mc, int numParts) {
		_partitioner = new ColPartitioner(mc, numParts);
	}

	@Override
	public int getPartition(Object key) {
//		int k = _partitioner.getPartition(new MatrixIndexes(1, (long) key));
//		return hash(k) % _partitioner._numParts;
		return _partitioner.getPartition(new MatrixIndexes(1, (long) key));
	}

	@Override
	public int numPartitions() {
		return _partitioner.numPartitions();
	}

	private static int hash(int h) {
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

}
