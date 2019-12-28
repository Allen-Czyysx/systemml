package org.apache.sysml.runtime.instructions.spark.data;

import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class RowPartitioner extends BlockPartitioner {

	private static final long serialVersionUID = 3207938407732880324L;

	public RowPartitioner(MatrixCharacteristics mc, int numParts) {
		super(mc, numParts);

		if (!mc.dimsKnown() || mc.getRowsPerBlock() < 1 || mc.getColsPerBlock() < 1) {
			throw new RuntimeException("Invalid unknown matrix characteristics.");
		}

		long nrblks = mc.getNumRowBlocks();
		long ncblks = mc.getNumColBlocks();
		long nblks = nrblks * ncblks;

		double nblksPerPart = Math.max((double) nblks / numParts, 1);

		_rbPerPart = (long) Math.max(Math.floor(nblksPerPart / ncblks), 1);
		_cbPerPart = (long) Math.max(Math.floor(nblksPerPart / _rbPerPart), 1);

		_ncparts = (int) Math.ceil((double) ncblks / _cbPerPart);
		_numParts = numParts;
	}

	public static void main(String[] args) {
		MatrixCharacteristics mc =
				new MatrixCharacteristics(2300000, 3200000, 1000, 1000, 277100000);
		int numParts = 725;
		ColPartitioner partitioner = new ColPartitioner(mc, numParts);

		MatrixIndexes idx = new MatrixIndexes(2300, 4);
		System.out.println(partitioner.getPartition(idx));
	}

}
