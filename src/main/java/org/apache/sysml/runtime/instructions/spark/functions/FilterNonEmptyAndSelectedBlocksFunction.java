package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import scala.Tuple2;

public class FilterNonEmptyAndSelectedBlocksFunction implements Function<Tuple2<MatrixIndexes, MatrixBlock>, Boolean> {

	private static final long serialVersionUID = -8856829325565589854L;

	private final PartitionedBroadcast<MatrixBlock> _in2;

	public FilterNonEmptyAndSelectedBlocksFunction(PartitionedBroadcast<MatrixBlock> in2) {
		_in2 = in2;
	}

	@Override
	public Boolean call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
		long rowIdx = arg0._1().getRowIndex();
		long colIdx = arg0._1().getColumnIndex();

		boolean ix1 = rowIdx == 1 && colIdx == 1;

		boolean isSelected = true;
		for (int j = 1; j <= _in2.getNumColumnBlocks(); j++) {
			isSelected &= !_in2.getBlock((int) colIdx, j).isEmptyBlock(false);
		}

		return !arg0._2().isEmptyBlock(false) && isSelected || ix1;
	}

}
