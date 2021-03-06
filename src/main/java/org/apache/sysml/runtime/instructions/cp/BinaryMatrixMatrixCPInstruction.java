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

package org.apache.sysml.runtime.instructions.cp;

import org.apache.sysml.parser.DWhileStatement;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.functionobjects.MultiplyBlock;
import org.apache.sysml.runtime.matrix.data.LibCommonsMath;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.matrix.operators.BinaryOperator;
import org.apache.sysml.runtime.matrix.operators.Operator;

public class BinaryMatrixMatrixCPInstruction extends BinaryCPInstruction {

	protected BinaryMatrixMatrixCPInstruction(Operator op, CPOperand in1, CPOperand in2, CPOperand out,
			String opcode, String istr) {
		super(CPType.Binary, op, in1, in2, out, opcode, istr);
	}

	@Override
	public void processInstruction(ExecutionContext ec) {
		// TODO added by czh debug
		long t1 = System.currentTimeMillis();

		if ( LibCommonsMath.isSupportedMatrixMatrixOperation(getOpcode()) ) {
			MatrixBlock solution = LibCommonsMath.matrixMatrixOperations(
				ec.getMatrixInput(input1.getName()), ec.getMatrixInput(input2.getName()), getOpcode());
			ec.setMatrixOutput(output.getName(), solution, getExtendedOpcode());
			ec.releaseMatrixInput(input1.getName());
			ec.releaseMatrixInput(input2.getName());
			return;
		}
		
		// Read input matrices
		MatrixBlock inBlock1 = ec.getMatrixInput(input1.getName(), getExtendedOpcode());
		MatrixBlock inBlock2 = ec.getMatrixInput(input2.getName(), getExtendedOpcode());
		
		// Perform computation using input matrices, and produce the result matrix
		BinaryOperator bop = (BinaryOperator) _optr;
		MatrixBlock retBlock;
		if (bop.fn.isBlockFn()) {
			if (bop.fn instanceof MultiplyBlock) {
				retBlock = new MatrixBlock(inBlock1);
				retBlock.setSelectBlock(inBlock2);
			} else {
				throw new DMLRuntimeException("Shouldn't be here");
			}

		} else {
			retBlock = (MatrixBlock) (inBlock1.binaryOperations (bop, inBlock2, new MatrixBlock()));
		}

		// Release the memory occupied by input matrices
		ec.releaseMatrixInput(input1.getName(), getExtendedOpcode());
		ec.releaseMatrixInput(input2.getName(), getExtendedOpcode());
		
		// Ensure right dense/sparse output representation (guarded by released input memory)
		if( checkGuardedRepresentationChange(inBlock1, inBlock2, retBlock) ) {
			retBlock.examSparsity();
		}
		
		// Attach result matrix with MatrixObject associated with output_name
		ec.setMatrixOutput(output.getName(), retBlock, getExtendedOpcode());

		// TODO added by czh debug
//		if (DWhileStatement.isPreOutputNameFromHop(input1.getName())) {
			long t2 = System.currentTimeMillis();
			System.out.println(getOpcode() + " " + input1.getName() + " " + input2.getName() + " " + output.getName()
					+ " time: " + (t2 - t1) / 1000.0);
//		}
	}
}