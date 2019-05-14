package org.apache.sysml.parser;

import org.apache.sysml.hops.Hop;
import org.apache.sysml.runtime.instructions.Instruction;

import java.util.ArrayList;

public class DWhileStatement extends WhileStatement {

	public static final int CACHE_PERIOD = 10;

	private String[] _dVarNames;

	private ArrayList<StatementBlock> _dIterInit;

	private ArrayList<StatementBlock> _dIterBefore;

	private ArrayList<StatementBlock> _dIterAfter;

	public String[] getDVarNames() {
		return _dVarNames;
	}

	public void setDVarNames(String[] dVarName) {
		_dVarNames = dVarName;
	}

	public ArrayList<StatementBlock> getDIterInit() {
		return _dIterInit;
	}

	public StatementBlock getDIterInitByIndex(int i) {
		return _dIterInit.get(i);
	}

	public void setDIterInit(ArrayList<StatementBlock> dIterInit) {
		_dIterInit = dIterInit;
	}

	public ArrayList<StatementBlock> getDIterBefore() {
		return _dIterBefore;
	}

	public StatementBlock getDIterBeforeByIndex(int i) {
		return _dIterBefore.get(i);
	}

	public void setDIterBefore(ArrayList<StatementBlock> dIterBefore) {
		_dIterBefore = dIterBefore;
	}

	public ArrayList<StatementBlock> getDIterAfter() {
		return _dIterAfter;
	}

	public StatementBlock getDIterAfterByIndex(int i) {
		return _dIterAfter.get(i);
	}

	public void setDIterAfter(ArrayList<StatementBlock> dIterAfter) {
		_dIterAfter = dIterAfter;
	}

	public static boolean isDVar(String name, String[] curDVars) {
		for (String curDVarName : curDVars) {
			if (name.equals(curDVarName)) {
				return true;
			}
		}
		return false;
	}

	public static boolean isDWhileTmpVar(String varName) {
		if (varName == null) {
			return false;
		}
		return Character.isDigit(varName.charAt(0));
	}

	public static String getPreVarName(String varName) {
		return "1_preVar_" + varName;
	}

	public static String getVarUseDeltaName(String varName) {
		return "2_useDelta_" + varName;
	}

	public static String getSelectName(String varName) {
		return "3_select_" + varName;
	}

	public static String getDeltaName(String varName) {
		return "4_delta_" + varName;
	}

	public static String getPreOutputNameFromHop(Hop hop) {
		return "5_preOutput_hop_" + hop.getName() + "_" + hop.getOpString() + "_" + hop.getBeginLine() + "_"
				+ hop.getBeginColumn() + "_" + hop.getEndLine() + "_" + hop.getEndColumn();
	}

	public static String getPreOutputNameFromInst(Instruction inst) {
		return "6_preOutput_inst_" + inst.getOpcode() + "_" + inst.getBeginLine() + "_" + inst.getBeginColumn() + "_"
				+ inst.getEndLine() + "_" + inst.getEndColumn();
	}

	public static String getUseDeltaCountName(String varName) {
		return "6_useDeltaCount_" + varName;
	}

	public static String getSelectBlockNum(String varName) {
		return "7_blockSum_" + varName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("dwhile (");
		sb.append(_predicate);
		sb.append(" @ ");
		for (String name : _dVarNames) {
			sb.append(name);
			sb.append(", ");
		}
		sb.append(") {\n");
		for (StatementBlock block : _body) {
			sb.append(block.toString());
		}
		sb.append("}\n");
		return sb.toString();
	}

}
