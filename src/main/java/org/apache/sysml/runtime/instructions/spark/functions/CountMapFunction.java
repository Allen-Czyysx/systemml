package org.apache.sysml.runtime.instructions.spark.functions;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.matrix.data.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;

public class CountMapFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes, MatrixBlock>>,
		Integer, Tuple3<Integer, Integer, Long>> {

	private static final long serialVersionUID = 1886318890063064287L;

	private final PartitionedBroadcast<MatrixBlock> _pbc;

	public CountMapFunction(PartitionedBroadcast<MatrixBlock> binput) {
		_pbc = binput;
	}

	@Override
	public Iterator<Tuple2<Integer, Tuple3<Integer, Integer, Long>>> call(
			Iterator<Tuple2<MatrixIndexes, MatrixBlock>> iterator) throws Exception {
		Map<MatrixIndexes, MatrixBlock> outputMap = new HashMap<>();
		int denseCount = 0;
		int sparseCount = 0;
		long memCost = 0;

//		while (iterator.hasNext()) {
//			Tuple2<MatrixIndexes, MatrixBlock> arg = iterator.next();
//			MatrixIndexes ixIn = arg._1();
//			MatrixBlock blkIn = arg._2();
//
//			memCost -= blkIn.estimateSizeInMemory();
//
//			MatrixBlock mb = new MatrixBlock(blkIn.getNumRows(), blkIn.getNumColumns(), blkIn.isInSparseFormat(),
//					blkIn.getNonZeros());
//			outputMap.put(ixIn, mb);
//
//			if (blkIn.getDenseBlock() == null && blkIn.getSparseBlock() == null) {
//				continue;
//			}
//
//			if (blkIn.isInSparseFormat()) {
//				denseCount += blkIn.getSparseBlock().size();
//				for (Iterator<IJV> it = blkIn.getSparseBlockIterator(); it.hasNext(); ) {
//					IJV ijv = it.next();
//					int i = ijv.getI();
//					int j = ijv.getJ();
//					double v = ijv.getV();
//					mb.quickSetValue(i, j, v);
//				}
//
//			} else {
//				for (int j = 0; j < blkIn.getNumColumns(); j++) {
//					for (int i = 0; i < blkIn.getNumRows(); i++) {
//						double v = blkIn.getValue(i, j);
//						mb.quickSetValue(i, j, v);
//					}
//				}
//			}
//		}

		while (iterator.hasNext()) {
			Tuple2<MatrixIndexes, MatrixBlock> arg = iterator.next();
			MatrixIndexes ixIn = arg._1();
			MatrixBlock blkIn = arg._2();
			int blockI = (int) ixIn.getRowIndex();
			int blockJ = (int) ixIn.getColumnIndex();
			MatrixBlock colOrder = _pbc.getBlock(blockJ, 1);

			memCost -= blkIn.estimateSizeInMemory();

			if (blkIn.getDenseBlock() == null && blkIn.getSparseBlock() == null) {
				continue;
			}

			if (blkIn.isInSparseFormat()) {
				for (Iterator<IJV> it = blkIn.getSparseBlockIterator(); it.hasNext(); ) {
					IJV ijv = it.next();
					int i = ijv.getI();
					int j = ijv.getJ();
					int newGlobalJ = (int) colOrder.getValue(j, 0);
					double v = ijv.getV();

					setValueToMap(outputMap, getIndexes(blockI, newGlobalJ), i, getIndexInBlock(newGlobalJ), v,
							blkIn.getNumRows(), blkIn.getNonZeros(), true);
				}

			} else {
				for (int j = 0; j < blkIn.getNumColumns(); j++) {
					for (int i = 0; i < blkIn.getNumRows(); i++) {
						int newGlobalJ = (int) colOrder.getValue(j, 0);
						double v = blkIn.getValue(i, j);

						setValueToMap(outputMap, getIndexes(blockI, newGlobalJ), i, getIndexInBlock(newGlobalJ), v,
								blkIn.getNumRows(), blkIn.getNonZeros(), false);
					}
				}
			}
		}

		List<Tuple2<Integer, Tuple3<Integer, Integer, Long>>> ret = new ArrayList<>(outputMap.size());
		for (Map.Entry entry : outputMap.entrySet()) {
			MatrixBlock blk = (MatrixBlock) entry.getValue();

			blk.examSparsity();
			if (blk.isInSparseFormat() && !blk.isUltraSparse() && !(blk.getSparseBlock() instanceof SparseBlockCSR)) {
				sparseCount++;
				blk = new MatrixBlock(blk, MatrixBlock.DEFAULT_INPLACE_SPARSEBLOCK, true);
			}

			memCost += blk.estimateSizeInMemory();
		}
		ret.add(new Tuple2<>(0, new Tuple3<>(denseCount, sparseCount, memCost)));

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
			map.put(idx, block);

		} else {
			block = map.get(idx);
		}
		block.quickSetValue(i, j, v);
	}

}
