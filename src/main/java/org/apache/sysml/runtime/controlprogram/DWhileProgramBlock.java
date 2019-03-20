package org.apache.sysml.runtime.controlprogram;

import org.apache.sysml.api.DMLScript;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.parser.DWhileStatementBlock;
import org.apache.sysml.runtime.DMLRuntimeException;
import org.apache.sysml.runtime.DMLScriptException;
import org.apache.sysml.runtime.controlprogram.caching.MatrixObject;
import org.apache.sysml.runtime.controlprogram.context.ExecutionContext;
import org.apache.sysml.runtime.instructions.Instruction;
import org.apache.sysml.yarn.DMLAppMasterUtils;

import java.util.ArrayList;

public class DWhileProgramBlock extends WhileProgramBlock {

	private ArrayList<Instruction> _dIterBefore;

	private ArrayList<Instruction> _dIterAfter;

	public DWhileProgramBlock(Program prog,
							  ArrayList<Instruction> predicate,
							  ArrayList<Instruction> dIterBefore,
							  ArrayList<Instruction> dIterAfter) {
		super(prog, predicate);
		_dIterBefore = dIterBefore;
		_dIterAfter = dIterAfter;
	}

	public ArrayList<Instruction> getDIterBefore() {
		return _dIterBefore;
	}

	public void setDIterBefore(ArrayList<Instruction> dIterBefore) {
		_dIterBefore = dIterBefore;
	}

	public ArrayList<Instruction> getDIterAfter() {
		return _dIterAfter;
	}

	public void setDIterAfter(ArrayList<Instruction> dIterAfter) {
		_dIterAfter = dIterAfter;
	}

	private void executeDIterBefore(ExecutionContext ec) {
		try {
			// set program block specific remote memory
			if (DMLScript.isActiveAM()) {
				DMLAppMasterUtils.setupProgramBlockRemoteMaxMemory(this);
			}

			DWhileStatementBlock dwsb = (DWhileStatementBlock) _sb;
			ArrayList<Hop> hops = dwsb.getDIterBeforeHops();
			boolean recompile = dwsb.requiresDIterBeforeRecompilation();
			executeDWhile(getDIterBefore(), hops, recompile, ec);
		} catch (Exception e) {
			throw new DMLRuntimeException(this.printBlockErrorLocation() + "Failed to evaluate the while predicate.", e);
		}
	}

	private void executeDIterAfter(ExecutionContext ec) {
		try {
			// set program block specific remote memory
			if (DMLScript.isActiveAM()) {
				DMLAppMasterUtils.setupProgramBlockRemoteMaxMemory(this);
			}

			DWhileStatementBlock dwsb = (DWhileStatementBlock) _sb;
			ArrayList<Hop> hops = dwsb.getDIterAfterHops();
			boolean recompile = dwsb.requiresDIterAfterRecompilation();
			executeDWhile(getDIterAfter(), hops, recompile, ec);
		} catch (Exception e) {
			throw new DMLRuntimeException(this.printBlockErrorLocation() + "Failed to evaluate the while predicate.", e);
		}
	}

	@Override
	public void execute(ExecutionContext ec) {
		// execute while loop
		try {
			// prepare update in-place variables
			MatrixObject.UpdateType[] flags = prepareUpdateInPlaceVariables(ec, _tid);

			// run loop body until predicate becomes false
			while (executePredicate(ec).getBooleanValue()) {
				// 执行dBefore
				executeDIterBefore(ec);

				// execute all child blocks
				for (int i = 0; i < _childBlocks.size(); i++) {
					ec.updateDebugState(i);
					_childBlocks.get(i).execute(ec);
				}

				// 执行dAfter
				executeDIterAfter(ec);
			}

			// reset update-in-place variables
			resetUpdateInPlaceVariableFlags(ec, flags);
		} catch (DMLScriptException e) {
			// propagate stop call
			throw e;
		} catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error evaluating while program block", e);
		}

		// execute exit instructions
		try {
			executeInstructions(_exitInstructions, ec);
		} catch (Exception e) {
			throw new DMLRuntimeException(printBlockErrorLocation() + "Error executing while exit instructions.", e);
		}
	}

	@Override
	public String printBlockErrorLocation() {
		return "ERROR: Runtime error in dwhile program block generated from dwhile statement block between lines "
				+ _beginLine + " and " + _endLine + " -- ";
	}

}