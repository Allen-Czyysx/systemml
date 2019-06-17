package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
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
		Map<MatrixIndexes, MatrixBlock> blkOut = new HashMap<>();

		while (iterator.hasNext()) {
			Tuple2<MatrixIndexes, MatrixBlock> arg = iterator.next();
			MatrixIndexes ixIn = arg._1();
			MatrixBlock blkIn = arg._2();
			int blockI = (int) ixIn.getRowIndex();
			int blockJ = (int) ixIn.getColumnIndex();
			MatrixBlock colOrder = _pbc.getBlock(blockJ, 1);

			if (blkIn.getDenseBlock() == null && blkIn.getSparseBlock() == null) {
				return null;
			}

			if (blkIn.isInSparseFormat()) {
				for (Iterator<IJV> it = blkIn.getSparseBlockIterator(); it.hasNext(); ) {
					IJV ijv = it.next();
					int i = ijv.getI();
					int j = ijv.getJ();
					int newGlobalJ = (int) colOrder.getValue(j, 0);

					if (newGlobalJ == 0) {
						continue;
					}

					double v = ijv.getV();
					setValueToMap(blkOut, getIndexes(blockI, newGlobalJ == -1 ? 0 : newGlobalJ), i,
							getIndexInBlock(newGlobalJ == -1 ? 0 : newGlobalJ), v, true);
				}

			} else {
				for (int j = 0; j < blkIn.getNumColumns(); j++) {
					int newGlobalJ = (int) colOrder.getValue(j, 0);

					if (newGlobalJ == 0) {
						continue;
					}

					for (int i = 0; i < blkIn.getNumRows(); i++) {
						double v = blkIn.getValue(i, j);
						setValueToMap(blkOut, getIndexes(blockI, newGlobalJ == -1 ? 0 : newGlobalJ), i,
								getIndexInBlock(newGlobalJ == -1 ? 0 : newGlobalJ), v, false);
					}
				}
			}
		}

		List<Tuple2<MatrixIndexes, MatrixBlock>> ret = new ArrayList<>(blkOut.size());
		for (Map.Entry entry : blkOut.entrySet()) {
			MatrixIndexes idx = (MatrixIndexes) entry.getKey();
			MatrixBlock blk = (MatrixBlock) entry.getValue();
			ret.add(new Tuple2<>(idx, blk));
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
							   boolean sparse) {
		if (v == 0) {
			return;
		}

		MatrixBlock block;
		if (!map.containsKey(idx)) {
			if (idx.getRowIndex() * 1000 > _pbc.getNumRows()) {
				block = new MatrixBlock((int) _pbc.getNumRows() % DEFAULT_BLOCKSIZE, DEFAULT_BLOCKSIZE, sparse);
			} else {
				block = new MatrixBlock(DEFAULT_BLOCKSIZE, DEFAULT_BLOCKSIZE, sparse);
			}
			if (sparse) {
				block.allocateSparseRowsBlock();
			} else {
				block.allocateDenseBlock();
			}
			map.put(idx, block);
		} else {
			block = map.get(idx);
		}

		block.setValue(i, j, v);
	}

}
