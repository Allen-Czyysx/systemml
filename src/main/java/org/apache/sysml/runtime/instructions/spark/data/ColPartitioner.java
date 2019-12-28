package org.apache.sysml.runtime.instructions.spark.data;

import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;

public class ColPartitioner extends RowPartitioner {

	private static final long serialVersionUID = 3207938407732880324L;

	private int _nrparts;

	public ColPartitioner(MatrixCharacteristics mc, int numParts) {
		super(new MatrixCharacteristics(mc.getCols(), mc.getRows(), mc.getColsPerBlock(), mc.getRowsPerBlock(),
						mc.getNonZeros()), numParts);
		_nrparts = (int) Math.ceil((double) mc.getNumRowBlocks() / _rbPerPart);
	}

	@Override
	public int getPartition(Object arg0) {
		if (!(arg0 instanceof MatrixIndexes)) {
			throw new RuntimeException("Unsupported key class (expected MatrixIndexes): " + arg0.getClass().getName());
		}

		MatrixIndexes idx = (MatrixIndexes) arg0;
		MatrixIndexes newIdx = new MatrixIndexes(idx.getColumnIndex(), idx.getRowIndex());

		int ixr = (int) ((newIdx.getRowIndex() - 1) / _rbPerPart);
		int ixc = (int) ((newIdx.getColumnIndex() - 1) / _cbPerPart);
		int id = ixc * _nrparts + ixr;

		return id % _numParts;
	}

}
