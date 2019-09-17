package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.*;
import scala.Tuple2;

import java.util.*;

import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;

public class RepartitionMapFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>,
		MatrixIndexes, MatrixBlock> {

	private static final long serialVersionUID = 1886318890063064287L;

	private final PartitionedBroadcast<MatrixBlock> _pbc;

	public RepartitionMapFunction(PartitionedBroadcast<MatrixBlock> binput) {
		_pbc = binput;
	}

	@Override
	public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> iterator)
			throws Exception {
		Map<MatrixIndexes, MatrixBlock> outputMap = new HashMap<>();

		while (iterator.hasNext()) {
			Tuple2<MatrixIndexes, MatrixBlock> arg = iterator.next();
			MatrixIndexes ixIn = arg._1();
			MatrixBlock blkIn = arg._2();
			int blockI = (int) ixIn.getRowIndex();
			int blockJ = (int) ixIn.getColumnIndex();
			MatrixBlock colOrder = _pbc.getBlock(blockJ, 1);

			if (!(ixIn.getRowIndex() == 1 && ixIn.getColumnIndex() == 1) && blkIn.isEmptyBlock(false)) {
				continue;
			}

			if (blkIn.isInSparseFormat()) {
				for (Iterator<IJV> it = blkIn.getSparseBlockIterator(); it.hasNext(); ) {
					IJV ijv = it.next();
					int i = ijv.getI();
					int j = ijv.getJ();
					double v = ijv.getV();
					int newGlobalJ = (int) colOrder.getValue(j, 0);

					if (newGlobalJ < 0) {
						continue;
					}

					setValueToMap(outputMap, getIndexes(blockI, newGlobalJ), i, getIndexInBlock(newGlobalJ), v,
							blkIn.getNumRows(), blkIn.getNonZeros(), true);
				}

			} else {
				for (int j = 0; j < blkIn.getNumColumns(); j++) {
					for (int i = 0; i < blkIn.getNumRows(); i++) {
						double v = blkIn.getValue(i, j);
						int newGlobalJ = (int) colOrder.getValue(j, 0);

						if (newGlobalJ < 0) {
							continue;
						}

						setValueToMap(outputMap, getIndexes(blockI, newGlobalJ), i, getIndexInBlock(newGlobalJ), v,
								blkIn.getNumRows(), blkIn.getNonZeros(), false);
					}
				}
			}
		}

		List<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<>(outputMap.size());
		for (Map.Entry<MatrixIndexes, MatrixBlock> entry : outputMap.entrySet()) {
			MatrixIndexes idx = entry.getKey();
			MatrixBlock blk = entry.getValue();
			blk.examSparsity();

			if ((idx.getRowIndex() == 1 && idx.getColumnIndex() == 1) || !blk.isEmptyBlock(false)) {
				if (blk.isInSparseFormat() && !blk.isUltraSparse() && !(blk.getSparseBlock() instanceof SparseBlockCSR)) {
					entry.setValue(null);
					blk = new MatrixBlock(blk, MatrixBlock.DEFAULT_INPLACE_SPARSEBLOCK, true);
				}
				ret.add(new Tuple2<>(idx, blk));
			}
		}

		return ret.iterator();
	}

	private MatrixIndexes getIndexes(int blockI, int globalJ) {
		return new MatrixIndexes(blockI, globalJ / DEFAULT_BLOCKSIZE + 1);
	}

	private int getIndexInBlock(int globalI) {
		return globalI % DEFAULT_BLOCKSIZE;
	}

	private void setValueToMap(Map<MatrixIndexes, MatrixBlock> map, MatrixIndexes idx, int i, int j, double v,
							   int rowNumHint, long estnnzHint, boolean sparse) {
		if (v == 0) {
			return;
		}

		MatrixBlock block;
		if (!map.containsKey(idx)) {
			int rowNum = DEFAULT_BLOCKSIZE;
			int colNum = DEFAULT_BLOCKSIZE;
			if (rowNumHint > 0 && rowNumHint != DEFAULT_BLOCKSIZE) {
				rowNum = rowNumHint;
			}
			if (idx.getColumnIndex() * DEFAULT_BLOCKSIZE > _pbc.getNumRows()) {
				colNum = (int) _pbc.getNumRows() % DEFAULT_BLOCKSIZE;
			}

			block = new MatrixBlock(rowNum, colNum, sparse, estnnzHint);
			block.allocateBlock();
			map.put(idx, block);

		} else {
			block = map.get(idx);
		}
		block.setValue(i, j, v);
	}

}
