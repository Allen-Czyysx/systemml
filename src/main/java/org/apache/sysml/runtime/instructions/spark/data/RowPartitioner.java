package org.apache.sysml.runtime.instructions.spark.data;

import org.apache.sysml.runtime.matrix.MatrixCharacteristics;

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

		if (ncblks <= nblksPerPart) {
			_cbPerPart = ncblks;
			_rbPerPart = (long) Math.max(Math.floor(nblksPerPart / _cbPerPart), 1);
		} else {
			_rbPerPart = 1;
			_cbPerPart = (long) Math.max(Math.floor(nblksPerPart), 1);
		}

		_ncparts = (int) Math.ceil((double) ncblks / _cbPerPart);
		_numParts = numParts;

		_numColumnsPerBlock = mc.getColsPerBlock();
	}

}
