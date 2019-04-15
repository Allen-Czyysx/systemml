package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;

public class ExamSparsityFunction implements Function<MatrixBlock, MatrixBlock> {

	@Override
	public MatrixBlock call(MatrixBlock matrixBlock) throws Exception {
		matrixBlock.examSparsity();
		return matrixBlock;
	}

}
