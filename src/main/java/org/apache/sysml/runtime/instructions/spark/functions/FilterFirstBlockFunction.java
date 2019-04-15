package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.Function;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import scala.Tuple2;

public class FilterFirstBlockFunction implements Function<Tuple2<MatrixIndexes, MatrixBlock>, Boolean>
{

	@Override
	public Boolean call(Tuple2<MatrixIndexes, MatrixBlock> arg0) throws Exception {
		return arg0._1().getRowIndex() == 1;
	}

}
