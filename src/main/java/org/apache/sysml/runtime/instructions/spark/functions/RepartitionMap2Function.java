package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.*;
import scala.Tuple2;

import java.util.*;

import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;

public class RepartitionMap2Function implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>,
		MatrixIndexes, MatrixBlock> {

	private static final long serialVersionUID = 1886318890063064287L;

	private final PartitionedBroadcast<MatrixBlock> _pbc;

	public RepartitionMap2Function(PartitionedBroadcast<MatrixBlock> binput) {
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
			MatrixBlock colOrder = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);
			MatrixBlock rowOrder = _pbc.getBlock((int) ixIn.getRowIndex(), 1);

			if (blkIn.getDenseBlock() == null && blkIn.getSparseBlock() == null) {
				return null;
			}

			if (blkIn.isInSparseFormat()) {
				SparseBlock in = blkIn.getSparseBlock();
				for (Iterator<IJV> it = in.getIterator(); it.hasNext(); ) {
					IJV ijv = it.next();
					int i = ijv.getI();
					int j = ijv.getJ();
					int newI = (int) rowOrder.getValue(i, 0);
					int newJ = (int) colOrder.getValue(j, 0);

					if (newI == 0 || newJ == 0) {
						continue;
					}

					double v = ijv.getV();
					setValueToMap(blkOut, getIndexes(newI == -1 ? 0 : newI, newJ == -1 ? 0 : newJ), i,
							getIndexInBlock(newJ == -1 ? 0 : newJ), v, true);
				}

			} else {
				DenseBlock in = blkIn.getDenseBlock();
				for (int j = 0; j < blkIn.getNumColumns(); j++) {
					int newJ = (int) colOrder.getValue(j, 0);

					if (newJ == 0) {
						continue;
					}

					for (int i = 0; i < blkIn.getNumRows(); i++) {
						int newI = (int) rowOrder.getValue(i, 0);

						if (newI == 0) {
							continue;
						}

						double v = in.get(i, j);
						setValueToMap(blkOut, getIndexes(newI == -1 ? 0 : newI, newJ == -1 ? 0 : newJ), i,
								getIndexInBlock(newJ == -1 ? 0 : newJ), v, false);
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

	private MatrixIndexes getIndexes(int i, int j) {
		return new MatrixIndexes(i / DEFAULT_BLOCKSIZE + 1, j / DEFAULT_BLOCKSIZE + 1);
	}

	private int getIndexInBlock(int i) {
		return i % DEFAULT_BLOCKSIZE;
	}

	private void setValueToMap(Map<MatrixIndexes, MatrixBlock> map, MatrixIndexes idx, int i, int j, double v,
							   boolean sparse) {
		if (v == 0) {
			return;
		}

		MatrixBlock block;
		if (!map.containsKey(idx)) {
			block = new MatrixBlock(DEFAULT_BLOCKSIZE, DEFAULT_BLOCKSIZE, sparse);
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
