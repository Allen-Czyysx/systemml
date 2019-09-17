/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysml.runtime.instructions.spark;


import java.util.*;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.sysml.parser.DWhileStatement;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.parser.ParseException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.spark.data.RDDObject;
import org.apache.sysml.runtime.instructions.spark.functions.*;
import org.apache.sysml.runtime.matrix.data.*;
import scala.Tuple2;

import org.apache.sysml.hops.AggBinaryOp.SparkAggType;
import org.apache.sysml.hops.OptimizerUtils;
import org.apache.sysml.lops.MapMult;
import org.apache.sysml.lops.MapMult.CacheType;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.controlprogram.context.SparkExecutionContext;
import org.apache.sysml.runtime.controlprogram.parfor.stat.InfrastructureAnalyzer;
import org.apache.sysml.runtime.functionobjects.Multiply;
import org.apache.sysml.runtime.functionobjects.Plus;
import org.apache.sysml.runtime.instructions.InstructionUtils;
import org.apache.sysml.runtime.instructions.cp.CPOperand;
import org.apache.sysml.runtime.instructions.spark.data.LazyIterableIterator;
import org.apache.sysml.runtime.instructions.spark.data.PartitionedBroadcast;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class MapmmSPInstruction extends BinarySPInstruction {

	private CacheType _type;
	private boolean _outputEmpty;
	private SparkAggType _aggtype;

	public static boolean useCP = false; // TODO added by czh

	private MapmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, CacheType type,
							   boolean outputEmpty, SparkAggType aggtype, String opcode, String istr,
							   boolean needCache, String preOutputName, String dVarName) {
		super(SPType.MAPMM, op, in1, in2, out, opcode, istr);
		_type = type;
		_outputEmpty = outputEmpty;
		_aggtype = aggtype;
		_needCache = needCache;
		_preOutputName = preOutputName;
		_dVarNames = new String[]{dVarName};
	}

	public static MapmmSPInstruction parseInstruction(String str) {
		String parts[] = InstructionUtils.getInstructionPartsWithValueType(str);
		String opcode = parts[0];

		if (!opcode.equalsIgnoreCase(MapMult.OPCODE))
			throw new DMLRuntimeException("MapmmSPInstruction.parseInstruction():: Unknown opcode " + opcode);

		CPOperand in1 = new CPOperand(parts[1]);
		CPOperand in2 = new CPOperand(parts[2]);
		CPOperand out = new CPOperand(parts[3]);
		CacheType type = CacheType.valueOf(parts[4]);
		boolean outputEmpty = Boolean.parseBoolean(parts[5]);
		SparkAggType aggtype = SparkAggType.valueOf(parts[6]);

		boolean needCache = Boolean.parseBoolean(parts[7]);
		String preOutputName = null;
		String dVarName = null;
		if (needCache) {
			preOutputName = parts[8];
			if (parts.length == 11 && parts[9].equals("1")) {
				dVarName = parts[10];
			} else if (parts.length > 11) {
				throw new ParseException("Should not be here");
			}
		}

		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		return new MapmmSPInstruction(
				aggbin, in1, in2, out, type, outputEmpty, aggtype, opcode, str, needCache, preOutputName, dVarName);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// TODO added by czh debug
		long t1 = System.currentTimeMillis();

		SparkExecutionContext sec = (SparkExecutionContext) ec;

		CacheType type = _type;
		String rddVar = type.isRight() ? input1.getName() : input2.getName();
		String bcastVar = type.isRight() ? input2.getName() : input1.getName();
		String repartitionRddVar = rddVar + "_repartition";
		String repartitionBcastVar = bcastVar + "_repartition";
		MatrixCharacteristics mcRdd = sec.getMatrixCharacteristics(rddVar);
		MatrixCharacteristics mcBc = sec.getMatrixCharacteristics(bcastVar);
		boolean needRepartitionNow = needRepartitionNow(bcastVar);
		boolean needFilterNow = needFilterNow(sec);
		String dVarName = null;
		if (_needCache && DWhileStatement.isDWhileTmpVar(bcastVar)) {
			dVarName = _dVarNames[0];
			System.out.println("in delta mapmm: " + dVarName);
		}

//		_outputEmpty = false; // TODO added by czh 删

		//get input rdd with preferred number of partitions to avoid unnecessary repartitionNonZeros
		JavaPairRDD<MatrixIndexes, MatrixBlock> in1;
		if (_needCache && (_hasRepartitioned.contains(rddVar) || _hasFiltered.contains(rddVar)) && !needRepartitionNow
				&& DWhileStatement.isDWhileTmpVar(bcastVar)) {
			// 在dwhile中, 做过repartition或filter且当前不需要repartition时取 repartitionRddVar
			in1 = sec.getBinaryBlockRDDHandleForVariable(repartitionRddVar,
					(requiresFlatMapFunction(type, mcBc) && requiresRepartitioning(
							type, mcRdd, mcBc, sec.getSparkContext().defaultParallelism())) ?
							getNumRepartitioning(type, mcRdd, mcBc) : -1, _outputEmpty);
		} else {
			in1 = sec.getBinaryBlockRDDHandleForVariable(rddVar,
					(requiresFlatMapFunction(type, mcBc) && requiresRepartitioning(
							type, mcRdd, mcBc, sec.getSparkContext().defaultParallelism())) ?
							getNumRepartitioning(type, mcRdd, mcBc) : -1, _outputEmpty);
		}

		//investigate if a repartitioning - including a potential flip of broadcast and rdd
		//inputs - is required to ensure moderately sized output partitions (2GB limitation)
		if (requiresFlatMapFunction(type, mcBc) && requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions())) {
			System.out.println("!!!!!"); // TODO added by czh debug
			int numParts = getNumRepartitioning(type, mcRdd, mcBc);
			int numParts2 = getNumRepartitioning(type.getFlipped(), mcBc, mcRdd);
			if (numParts2 > numParts) { //flip required
				type = type.getFlipped();
				rddVar = type.isRight() ? input1.getName() : input2.getName();
				bcastVar = type.isRight() ? input2.getName() : input1.getName();
				mcRdd = sec.getMatrixCharacteristics(rddVar);
				mcBc = sec.getMatrixCharacteristics(bcastVar);
				in1 = sec.getBinaryBlockRDDHandleForVariable(rddVar);
				LOG.warn("Mapmm: Switching rdd ('" + bcastVar + "') and broadcast ('" + rddVar + "') inputs "
						+ "for repartitioning because this allows better control of output partition "
						+ "sizes (" + numParts + " < " + numParts2 + ").");
			}
		}

		if (needRepartitionNow) {
			System.out.println("repartitioning... " + bcastVar);
			_hasRepartitioned.add(rddVar);

			if (sec.containsVariable(repartitionRddVar)) {
				sec.cleanupCacheableData(sec.getMatrixObject(repartitionRddVar));
			}

			// repartitionNonZeros in2
			MatrixBlock order =
					LibMatrixReorg.getOrderWithSelect(sec, true, dVarName, in1.getNumPartitions(), mcRdd);
			if (order == null) {
				skip(sec);
				return;
			}
			MatrixBlock newIn2Block;
			if (sec.getMatrixObject(bcastVar).getRDDHandle() != null) {
				RDDObject preRO = sec.getMatrixObject(bcastVar).getRDDHandle();
				JavaPairRDD in2 = preRO.getRDD();
				String orderName = DWhileStatement.getRepartitionOrderName(dVarName);
				PartitionedBroadcast<MatrixBlock> orderPb = sec.getBroadcastForVariable(orderName);
				JavaPairRDD<MatrixIndexes, MatrixBlock> tmp2 =
						LibMatrixReorg.repartitionIn2ByOrderSP(in2, mcBc, orderPb);
				sec.getMatrixObject(bcastVar).setRDDHandle(new RDDObject(tmp2));
				newIn2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
				sec.getMatrixObject(bcastVar).setRDDHandle(preRO);

			} else {
				MatrixBlock in2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
				newIn2Block = LibMatrixReorg.repartitionIn2ByOrderCP(in2Block, order);
			}

			if (!sec.containsVariable(repartitionBcastVar)) {
				sec.setVariable(repartitionBcastVar, new MatrixObject(sec.getMatrixObject(bcastVar)));
			}
			sec.cleanupBroadcastByDWhile(sec.getMatrixObject(repartitionBcastVar));
			sec.setMatrixOutput(repartitionBcastVar, newIn2Block);

			// repartitionNonZeros in1
			in1 = LibMatrixReorg.repartitionIn1ByOrderSP(sec, in1, dVarName, rddVar, repartitionRddVar);

		} else if (hasRepartitioned(rddVar, bcastVar)) {
			// 若在之前的迭代已repartition in1, 则调整 in2 顺序
			MatrixBlock order = LibMatrixReorg.getNewOrderWithSelect(sec, true, dVarName);
			if (order == null) {
				skip(sec);
				return;
			}
			MatrixBlock newIn2Block;
			if (sec.getMatrixObject(bcastVar).getRDDHandle() != null) {
				RDDObject preRO = sec.getMatrixObject(bcastVar).getRDDHandle();
				JavaPairRDD in2 = preRO.getRDD();
				String newOrderName = DWhileStatement.getNewRepartitionOrderName(dVarName);
				PartitionedBroadcast<MatrixBlock> newOrderPb = sec.getBroadcastForVariable(newOrderName);
				JavaPairRDD<MatrixIndexes, MatrixBlock> tmp2 =
						LibMatrixReorg.repartitionIn2ByOrderSP(in2, mcBc, newOrderPb);
				sec.getMatrixObject(bcastVar).setRDDHandle(new RDDObject(tmp2));
				newIn2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
				sec.getMatrixObject(bcastVar).setRDDHandle(preRO);

			} else {
				MatrixBlock in2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
				newIn2Block = LibMatrixReorg.repartitionIn2ByOrderCP(in2Block, order);
			}

			if (!ec.containsVariable(repartitionBcastVar)) {
				sec.setVariable(repartitionBcastVar, new MatrixObject(sec.getMatrixObject(bcastVar)));
			}
			sec.unpersistBroadcastByDWhile(sec.getMatrixObject(repartitionBcastVar));
			sec.setMatrixOutput(repartitionBcastVar, newIn2Block);

		} else if (_needCache && DWhileStatement.isDWhileTmpVar(bcastVar)) { // 设置 bcastVar.selectBlock
			FilterBlock selectBlock = selectMatrixBlock(sec, bcastVar, dVarName);
			if (selectBlock == null) {
				skip(sec);
				return;
			}
			MatrixBlock in2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
			in2Block.setSelectBlock(selectBlock);
			sec.setMatrixOutput(bcastVar, in2Block);
		}

		//get inputs
		if (hasRepartitioned(rddVar, bcastVar)) {
			bcastVar = repartitionBcastVar;
		}
		PartitionedBroadcast<MatrixBlock> in2 = sec.getBroadcastForVariable(bcastVar);

		//empty input block filter
		if (!_outputEmpty)
			in1 = in1.filter(new FilterNonEmptyBlocksFunction());

		if (needFilterNow) {
			System.out.print("filtering... " + bcastVar);
			_hasFiltered.add(rddVar);

			in1 = in1.filter(new FilterNonEmptyBlocksFunction());
			sec.persistRdd(rddVar, in1, StorageLevels.MEMORY_AND_DISK);
			if (!sec.containsVariable(repartitionRddVar)) {
				sec.setVariable(repartitionRddVar, new MatrixObject(sec.getMatrixObject(rddVar)));
			}
			sec.setRDDHandleForVariable(repartitionRddVar, in1);
			sec.addLineageBroadcast(repartitionRddVar, bcastVar);
			if (_hasRepartitioned.contains(rddVar) && DWhileStatement.isDWhileTmpVar(bcastVar)) {
				sec.addLineageBroadcast(repartitionRddVar, DWhileStatement.getRepartitionOrderName(dVarName));
			}

			System.out.println(" done");
		}

		//execute mapmm and aggregation if necessary and put output into symbol table
		if (_aggtype == SparkAggType.SINGLE_BLOCK) {
			JavaRDD<MatrixBlock> out = in1.map(new RDDMapMMFunction2(type, in2));
			MatrixBlock out2 = RDDAggregateUtils.sumStable(out);

			//put output block into symbol table (no lineage because single block)
			//this also includes implicit maintenance of matrix characteristics
			sec.setMatrixOutput(output.getName(), out2, getExtendedOpcode());

		} else { //MULTI_BLOCK or NONE
			JavaPairRDD<MatrixIndexes, MatrixBlock> out;
			if (requiresFlatMapFunction(type, mcBc)) {
				if (requiresRepartitioning(type, mcRdd, mcBc, in1.getNumPartitions())) {
					int numParts = getNumRepartitioning(type, mcRdd, mcBc);
					LOG.warn("Mapmm: Repartition input rdd '" + rddVar + "' from " + in1.getNumPartitions() + " to "
							+ numParts + " partitions to satisfy size restrictions of output partitions.");
					in1 = in1.repartition(numParts);
				}
				out = in1.flatMapToPair(new RDDFlatMapMMFunction(type, in2));
			} else if (preservesPartitioning(mcRdd, type))
				out = in1.mapPartitionsToPair(new RDDMapMMPartitionFunction(type, in2), true);
			else
				out = in1.mapToPair(new RDDMapMMFunction(type, in2));

			//empty output block filter
			if (!_outputEmpty)
				out = out.filter(new FilterNonEmptyBlocksFunction());

			if (_aggtype == SparkAggType.MULTI_BLOCK)
				out = RDDAggregateUtils.sumByKeyStable(out, false);

			//put output RDD handle into symbol table
			sec.setRDDHandleForVariable(output.getName(), out);
			sec.addLineageRDD(output.getName(), rddVar);
			sec.addLineageBroadcast(output.getName(), bcastVar);

			if (needRepartitionNow || needFilterNow) {
				sec.unpersistRdd(repartitionRddVar);
			}

			//update output statistics if not inferred
			updateBinaryMMOutputMatrixCharacteristics(sec, true);
		}

//		// TODO added by czh debug
//		sec.getMatrixInput(output.getName());
//		sec.releaseMatrixInput(output.getName());
//		long t2 = System.currentTimeMillis();
//		System.out.println("mapmm " + bcastVar + " time: " + (t2 - t1) / 1000.0);
	}

	private static boolean preservesPartitioning(MatrixCharacteristics mcIn, CacheType type) {
		if (type == CacheType.LEFT)
			return mcIn.dimsKnown() && mcIn.getRows() <= mcIn.getRowsPerBlock();
		else // RIGHT
			return mcIn.dimsKnown() && mcIn.getCols() <= mcIn.getColsPerBlock();
	}

	/**
	 * Indicates if there is a need to apply a flatmap rdd operation because a single
	 * input block creates multiple output blocks.
	 *
	 * @param type cache type
	 * @param mcBc matrix characteristics
	 * @return true if single input block creates multiple output blocks
	 */
	private static boolean requiresFlatMapFunction(CacheType type, MatrixCharacteristics mcBc) {
		return (type == CacheType.LEFT && mcBc.getRows() > mcBc.getRowsPerBlock())
				|| (type == CacheType.RIGHT && mcBc.getCols() > mcBc.getColsPerBlock());
	}

	/**
	 * Indicates if there is a need to repartitionNonZeros the input RDD in order to increase the
	 * degree of parallelism or reduce the output partition size (e.g., Spark still has a
	 * 2GB limitation of partitions)
	 *
	 * @param type          cache type
	 * @param mcRdd         rdd matrix characteristics
	 * @param mcBc          ?
	 * @param numPartitions number of partitions
	 * @return true need to repartitionNonZeros input RDD
	 */
	private static boolean requiresRepartitioning(CacheType type, MatrixCharacteristics mcRdd,
												  MatrixCharacteristics mcBc, int numPartitions) {
		//note: as repartitioning requires data shuffling, we try to be very conservative here
		//approach: we repartitionNonZeros, if there is a "outer-product-like" mm (single block common dimension),
		//the size of output partitions (assuming dense) exceeds a size of 1GB 

		boolean isLeft = (type == CacheType.LEFT);
		boolean isOuter = isLeft ?
				(mcRdd.getRows() <= mcRdd.getRowsPerBlock()) :
				(mcRdd.getCols() <= mcRdd.getColsPerBlock());
		boolean isLargeOutput = (OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft ? mcBc.getRows() :
						mcRdd.getRows(),
				isLeft ? mcRdd.getCols() : mcBc.getCols(), isLeft ? mcBc.getRowsPerBlock() : mcRdd.getRowsPerBlock(),
				isLeft ? mcRdd.getColsPerBlock() : mcBc.getColsPerBlock(), 1.0) / numPartitions) > 1024 * 1024 * 1024;
		return isOuter && isLargeOutput && mcRdd.dimsKnown() && mcBc.dimsKnown()
				&& numPartitions < getNumRepartitioning(type, mcRdd, mcBc);
	}

	/**
	 * Computes the number of target partitions for repartitioning input rdds in case of
	 * outer-product-like mm.
	 *
	 * @param type  cache type
	 * @param mcRdd rdd matrix characteristics
	 * @param mcBc  ?
	 * @return number of target partitions for repartitioning
	 */
	private static int getNumRepartitioning(CacheType type, MatrixCharacteristics mcRdd, MatrixCharacteristics mcBc) {
		boolean isLeft = (type == CacheType.LEFT);
		long sizeOutput = (OptimizerUtils.estimatePartitionedSizeExactSparsity(isLeft ? mcBc.getRows() :
						mcRdd.getRows(),
				isLeft ? mcRdd.getCols() : mcBc.getCols(), isLeft ? mcBc.getRowsPerBlock() : mcRdd.getRowsPerBlock(),
				isLeft ? mcRdd.getColsPerBlock() : mcBc.getColsPerBlock(), 1.0));
		long numParts = sizeOutput / InfrastructureAnalyzer.getHDFSBlockSize();
		return (int) Math.min(numParts, (isLeft ? mcRdd.getNumColBlocks() : mcRdd.getNumRowBlocks()));
	}

	private static class RDDMapMMFunction implements PairFunction<Tuple2<MatrixIndexes, MatrixBlock>, MatrixIndexes,
			MatrixBlock> {
		private static final long serialVersionUID = 8197406787010296291L;

		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;

		public RDDMapMMFunction(CacheType type, PartitionedBroadcast<MatrixBlock> binput) {
			_type = type;
			_pbc = binput;

			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Tuple2<MatrixIndexes, MatrixBlock> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
				throws Exception {
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			MatrixIndexes ixOut = new MatrixIndexes();
			MatrixBlock blkOut = new MatrixBlock();

			if (_type == CacheType.LEFT) {
				//get the right hand side matrix
				MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());

//				if (left.getSparseBlock() == null && left.getDenseBlock() == null) {
//					blkOut = new MatrixBlock();
//				} else {
					//execute matrix-vector mult
					OperationsOnMatrixValues.matMult(
							new MatrixIndexes(1, ixIn.getRowIndex()), left, ixIn, blkIn, ixOut, blkOut, _op);
//				}

			} else //if( _type == CacheType.RIGHT )
			{
				//get the right hand side matrix
				MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);

//				if (right.getSparseBlock() == null && right.getDenseBlock() == null) {
//					blkOut = new MatrixBlock();
//				} else {
					//execute matrix-vector mult
					OperationsOnMatrixValues.matMult(ixIn, blkIn,
							new MatrixIndexes(ixIn.getColumnIndex(), 1), right, ixOut, blkOut, _op);
//				}
			}

			//output new tuple
			return new Tuple2<>(ixOut, blkOut);
		}
	}

	/**
	 * Similar to RDDMapMMFunction but with single output block
	 */
	private static class RDDMapMMFunction2 implements Function<Tuple2<MatrixIndexes, MatrixBlock>, MatrixBlock> {
		private static final long serialVersionUID = -2753453898072910182L;

		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;

		public RDDMapMMFunction2(CacheType type, PartitionedBroadcast<MatrixBlock> binput) {
			_type = type;
			_pbc = binput;

			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public MatrixBlock call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
				throws Exception {
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			if (_type == CacheType.LEFT) {
				//get the right hand side matrix
				MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());

				//execute matrix-vector mult
				return OperationsOnMatrixValues.matMult(
						left, blkIn, new MatrixBlock(), _op);
			} else //if( _type == CacheType.RIGHT )
			{
				//get the right hand side matrix
				MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);

				//execute matrix-vector mult
				return OperationsOnMatrixValues.matMult(
						blkIn, right, new MatrixBlock(), _op);
			}
		}
	}

	private static class RDDMapMMPartitionFunction implements PairFlatMapFunction<Iterator<Tuple2<MatrixIndexes,
			MatrixBlock>>, MatrixIndexes, MatrixBlock> {
		private static final long serialVersionUID = 1886318890063064287L;

		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;

		public RDDMapMMPartitionFunction(CacheType type, PartitionedBroadcast<MatrixBlock> binput) {
			_type = type;
			_pbc = binput;

			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Iterator<Tuple2<MatrixIndexes,
				MatrixBlock>> arg0)
				throws Exception {
			return new MapMMPartitionIterator(arg0);
		}

		/**
		 * Lazy mapmm iterator to prevent materialization of entire partition output in-memory.
		 * The implementation via mapPartitions is required to preserve partitioning information,
		 * which is important for performance.
		 */
		private class MapMMPartitionIterator extends LazyIterableIterator<Tuple2<MatrixIndexes, MatrixBlock>> {
			public MapMMPartitionIterator(Iterator<Tuple2<MatrixIndexes, MatrixBlock>> in) {
				super(in);
			}

			@Override
			protected Tuple2<MatrixIndexes, MatrixBlock> computeNext(Tuple2<MatrixIndexes, MatrixBlock> arg) {
				MatrixIndexes ixIn = arg._1();
				MatrixBlock blkIn = arg._2();
				MatrixBlock blkOut = new MatrixBlock();

				if (_type == CacheType.LEFT) {
					//get the right hand side matrix
					MatrixBlock left = _pbc.getBlock(1, (int) ixIn.getRowIndex());

					//execute index preserving matrix multiplication
					OperationsOnMatrixValues.matMult(left, blkIn, blkOut, _op);
				} else //if( _type == CacheType.RIGHT )
				{
					//get the right hand side matrix
					MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);

					//execute index preserving matrix multiplication
					OperationsOnMatrixValues.matMult(blkIn, right, blkOut, _op);
				}

				return new Tuple2<>(ixIn, blkOut);
			}
		}
	}

	private static class RDDFlatMapMMFunction implements PairFlatMapFunction<Tuple2<MatrixIndexes, MatrixBlock>,
			MatrixIndexes, MatrixBlock> {
		private static final long serialVersionUID = -6076256569118957281L;

		private final CacheType _type;
		private final AggregateBinaryOperator _op;
		private final PartitionedBroadcast<MatrixBlock> _pbc;

		public RDDFlatMapMMFunction(CacheType type, PartitionedBroadcast<MatrixBlock> binput) {
			_type = type;
			_pbc = binput;

			//created operator for reuse
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			_op = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		}

		@Override
		public Iterator<Tuple2<MatrixIndexes, MatrixBlock>> call(Tuple2<MatrixIndexes, MatrixBlock> arg0)
				throws Exception {
			MatrixIndexes ixIn = arg0._1();
			MatrixBlock blkIn = arg0._2();

			if (_type == CacheType.LEFT) {
				//for all matching left-hand-side blocks, returned as lazy iterator
				return IntStream.range(1, _pbc.getNumRowBlocks() + 1).mapToObj(i ->
						new Tuple2<>(new MatrixIndexes(i, ixIn.getColumnIndex()),
								OperationsOnMatrixValues.matMult(_pbc.getBlock(i, (int) ixIn.getRowIndex()), blkIn,
										new MatrixBlock(), _op))).iterator();
			} else { //RIGHT
				//for all matching right-hand-side blocks, returned as lazy iterator
				return IntStream.range(1, _pbc.getNumColumnBlocks() + 1).mapToObj(j ->
						new Tuple2<>(new MatrixIndexes(ixIn.getRowIndex(), j),
								OperationsOnMatrixValues.matMult(blkIn, _pbc.getBlock((int) ixIn.getColumnIndex(), j),
										new MatrixBlock(), _op))).iterator();
			}
		}
	}

	/**
	 * @return 当前是否需要 filter
	 */
	boolean needFilterNow(SparkExecutionContext sec) {
		String bcastVar = _type.isRight() ? input2.getName() : input1.getName();

		if (!_needCache || !DWhileStatement.isDWhileTmpVar(bcastVar) && !DWhileStatement.isDVar(bcastVar, sec)) {
			return false;
		}

		String dVarName = DWhileStatement.getDVarNameFromTmpVar(bcastVar);
		String useFilterName = DWhileStatement.getUseFilterName(dVarName);
		if (sec.containsVariable(useFilterName)) {
			return sec.getScalarInput(useFilterName, Expression.ValueType.BOOLEAN, false)
					.getBooleanValue();
		}

		return false;
	}

	/**
	 * 跳过mapmm
	 */
	private void skip(SparkExecutionContext sec) {
		MatrixCharacteristics outputMc = sec.getMatrixCharacteristics(output.getName());
		sec.setMatrixOutput(output.getName(),
				new MatrixBlock((int) outputMc.getRows(), (int) outputMc.getCols(), true), getExtendedOpcode());

		if (_aggtype != SparkAggType.SINGLE_BLOCK) {
			updateBinaryMMOutputMatrixCharacteristics(sec, true);
		}

		// TODO added by czh debug
		System.out.println("skip mapmm " + input2.getName());
	}

}
