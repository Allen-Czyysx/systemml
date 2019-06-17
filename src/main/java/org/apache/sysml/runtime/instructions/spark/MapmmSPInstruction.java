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


import java.util.Iterator;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.sysml.parser.DWhileStatement;
import org.apache.sysml.parser.Expression;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.instructions.spark.data.BlockPartitioner;
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyAndSelectedBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.functions.RepartitionMapFunction;
import org.apache.sysml.runtime.matrix.data.LibMatrixReorg;
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
import org.apache.sysml.runtime.instructions.spark.functions.FilterNonEmptyBlocksFunction;
import org.apache.sysml.runtime.instructions.spark.utils.RDDAggregateUtils;
import org.apache.sysml.runtime.matrix.MatrixCharacteristics;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.data.MatrixIndexes;
import org.apache.sysml.runtime.matrix.data.OperationsOnMatrixValues;
import org.apache.sysml.runtime.matrix.operators.AggregateBinaryOperator;
import org.apache.sysml.runtime.matrix.operators.AggregateOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK;

public class MapmmSPInstruction extends BinarySPInstruction {

	private CacheType _type;
	private boolean _outputEmpty;
	private SparkAggType _aggtype;
	private  boolean _isSpecial;

	public static boolean hasRepartitioned = false;

	public static boolean hasFiltered = false;

	public static boolean useCP = false; // TODO added by czh

	private MapmmSPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out, CacheType type,
							   boolean outputEmpty, SparkAggType aggtype, String opcode, String istr, boolean isSpecial,
							   boolean needCache, String preOutputName) {
		super(SPType.MAPMM, op, in1, in2, out, opcode, istr);
		_type = type;
		_outputEmpty = outputEmpty;
		_aggtype = aggtype;
		_needCache = needCache;
		_preOutputName = preOutputName;
		_isSpecial = isSpecial;
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
		boolean isSpecial = Boolean.parseBoolean(parts[7]);

		boolean needCache = Boolean.parseBoolean(parts[8]);
		String preOutputName = needCache ? parts[9] : null;

		AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
		AggregateBinaryOperator aggbin = new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg);
		return new MapmmSPInstruction(
				aggbin, in1, in2, out, type, outputEmpty, aggtype, opcode, str, isSpecial, needCache, preOutputName);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		SparkExecutionContext sec = (SparkExecutionContext) ec;

		CacheType type = _type;
		String rddVar = type.isRight() ? input1.getName() : input2.getName();
		String bcastVar = type.isRight() ? input2.getName() : input1.getName();
		String betterRddVar = rddVar + "_filtered";
		String betterBcastVar = bcastVar + "_filtered";
		MatrixCharacteristics mcRdd = sec.getMatrixCharacteristics(rddVar);
		MatrixCharacteristics mcBc = sec.getMatrixCharacteristics(bcastVar);
		boolean needRepartitionNow = needRepartitionNow(sec);
		boolean needFilterNow = needFilterNow(sec);
		String dVarName = null; // TODO added by czh 检查是否是dVar
		if (_needCache && DWhileStatement.isDWhileTmpVar(bcastVar)) {
			dVarName = DWhileStatement.getDVarNameFromTmpVar(bcastVar);
		} else if (_needCache) {
			dVarName = bcastVar;
		}

		//get input rdd with preferred number of partitions to avoid unnecessary repartitionNonZeros
		JavaPairRDD<MatrixIndexes, MatrixBlock> in1;
		if (_needCache && (hasRepartitioned || hasFiltered) && !needRepartitionNow) {
			// 在dwhile中, 做过repartition或filter且当前不需要repartition时取 betterRddVar
			in1 = sec.getBinaryBlockRDDHandleForVariable(betterRddVar,
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

		if (_needCache && needRepartitionNow) {
			System.out.print("repartitioning...");
			hasRepartitioned = true;

			// repartitionNonZeros in2
			MatrixBlock in2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
			MatrixBlock repartitionSelect = sec.getMatrixObject(DWhileStatement.getRepartitionSelectName(dVarName))
					.acquireReadAndRelease();
			MatrixBlock newIn2Block = LibMatrixReorg.repartitionNonZeros(
					sec, in2Block, true, repartitionSelect, dVarName, in1.getNumPartitions(), mcRdd);
			if (!sec.containsVariable(betterBcastVar)) {
				sec.setVariable(betterBcastVar, new MatrixObject(sec.getMatrixObject(bcastVar)));
			}
			sec.cleanupBroadcastByDWhile(sec.getMatrixObject(betterBcastVar));
			sec.setMatrixOutput(betterBcastVar, newIn2Block);

			// repartitionNonZeros in1
			// TODO added by czh
//			MatrixCharacteristics in1MC = sec.getMatrixCharacteristics(rddVar);
//			if (OptimizerUtils.estimateSizeExactSparsity(in1MC.getRows(), newIn2Block.getNumRows(),
//					in1MC.getNonZeros()) <= OptimizerUtils.getLocalMemBudget()) {
			if (false) {
				System.out.println("begin local mapmm");
				useCP = true;
			} else {
				useCP = false;
			}

			String orderName = DWhileStatement.getRepartitionOrderName(dVarName);
			PartitionedBroadcast<MatrixBlock> order = sec.getBroadcastForVariable(orderName);
			in1 = in1.filter(new FilterNonEmptyBlocksFunction())
					.mapPartitionsToPair(new RepartitionMapFunction(order));
//			in1 = RDDAggregateUtils.mergeByKey(in1, false);
			if (!sec.containsVariable(betterRddVar)) {
				sec.setVariable(betterRddVar, new MatrixObject(sec.getMatrixObject(rddVar)));
			}
			sec.getMatrixCharacteristics(betterRddVar).setCols(newIn2Block.getNumRows());
			if (!useCP) {
				in1 = sec.persistRdd(rddVar, in1, MEMORY_AND_DISK);
				sec.setRDDHandleForVariable(betterRddVar, in1);
				sec.addLineageBroadcast(betterRddVar, orderName);
			} else {
				sec.setRDDHandleForVariable(betterRddVar, in1);
			}

			System.out.println("done");

		} else if (_needCache && hasRepartitioned) {
			// 若在之前的迭代已repartition in1, 则调整 in2 顺序
			MatrixBlock in2Block = sec.getMatrixObject(bcastVar).acquireReadAndRelease();
			MatrixBlock newIn2Block =
					LibMatrixReorg.repartitionByOrder(sec, in2Block, new MatrixBlock(), true, dVarName);
			if (!ec.containsVariable(betterBcastVar)) {
				MatrixObject mb = new MatrixObject(sec.getMatrixObject(bcastVar));
				ec.setVariable(betterBcastVar, mb);
			}
			sec.unpersistBroadcastByDWhile(sec.getMatrixObject(betterBcastVar));
			sec.setMatrixOutput(betterBcastVar, newIn2Block);
		}

		// TODO added by czh debug
		if (_needCache) {
			String preBlockNumName = DWhileStatement.getRepartitionPreBlockNumName(dVarName);
			long preBlockNum = sec.getScalarInput(preBlockNumName, Expression.ValueType.INT, false)
					.getLongValue();
			System.out.println("preBlockNumAfterRepartition = " + preBlockNum);
		}

		if (useCP) {
			MatrixBlock mb1 = sec.getMatrixInput(betterRddVar);
			MatrixBlock mb2 = sec.getMatrixInput(betterBcastVar);
			AggregateOperator agg = new AggregateOperator(0, Plus.getPlusFnObject());
			AggregateBinaryOperator aggbin =
					new AggregateBinaryOperator(Multiply.getMultiplyFnObject(), agg, 1);

			MatrixBlock ret = mb1.aggregateBinaryOperations(mb1, mb2, new MatrixBlock(), aggbin);

			sec.releaseMatrixInput(betterRddVar);
			sec.releaseMatrixInput(bcastVar);
			sec.setMatrixOutput(output.getName(), ret);

		} else {
			//get inputs
			if (_needCache && hasRepartitioned) {
				bcastVar = betterBcastVar;
			}
			PartitionedBroadcast<MatrixBlock> in2 = sec.getBroadcastForVariable(bcastVar);

			//empty input block filter
			if (!_outputEmpty)
				in1 = in1.filter(new FilterNonEmptyBlocksFunction());

			if (needFilterNow) {
				System.out.print("filtering...");
				hasFiltered = true;

				in1 = in1.filter(new FilterNonEmptyAndSelectedBlocksFunction(in2));
				sec.persistRdd(rddVar, in1, StorageLevels.MEMORY_AND_DISK);
				if (!sec.containsVariable(betterRddVar)) {
					sec.setVariable(betterRddVar, new MatrixObject(sec.getMatrixObject(rddVar)));
				}
				sec.setRDDHandleForVariable(betterRddVar, in1);
				sec.addLineageBroadcast(betterRddVar, bcastVar);
				if (hasRepartitioned) {
					sec.addLineageBroadcast(betterRddVar, DWhileStatement.getRepartitionOrderName(dVarName));
				}

				System.out.println("done");
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
					sec.unpersistRdd(rddVar);
				}

				//update output statistics if not inferred
				updateBinaryMMOutputMatrixCharacteristics(sec, true);
			}
		}
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

				//execute matrix-vector mult
				OperationsOnMatrixValues.matMult(new MatrixIndexes(1, ixIn.getRowIndex()),
						left, ixIn, blkIn, ixOut, blkOut, _op);
			} else //if( _type == CacheType.RIGHT )
			{
				//get the right hand side matrix
				MatrixBlock right = _pbc.getBlock((int) ixIn.getColumnIndex(), 1);

				//execute matrix-vector mult
				OperationsOnMatrixValues.matMult(ixIn, blkIn,
						new MatrixIndexes(ixIn.getColumnIndex(), 1), right, ixOut, blkOut, _op);
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
	 * @return 当前是否需要 repartition
	 */
	boolean needRepartitionNow(SparkExecutionContext sec) {
		String bcastVar = _type.isRight() ? input2.getName() : input1.getName();

		if (!_needCache || !DWhileStatement.isDWhileTmpVar(bcastVar) && !DWhileStatement.isDVar(bcastVar, sec)) {
			return false;
		}

		String dVarName = DWhileStatement.getDVarNameFromTmpVar(bcastVar);
		String useRepartitionName = DWhileStatement.getUseRepartitionName(dVarName);
		if (sec.containsVariable(useRepartitionName)) {
			return sec.getScalarInput(useRepartitionName, Expression.ValueType.BOOLEAN, false)
					.getBooleanValue();
		}

		return false;
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

}
