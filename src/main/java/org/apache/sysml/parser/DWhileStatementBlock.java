package org.apache.sysml.parser;

import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.lops.Lop;

import java.util.ArrayList;

public class DWhileStatementBlock extends WhileStatementBlock {

	private ArrayList<Hop> _dIterBeForeHops;
	private ArrayList<Hop> _dIterAfterHops;

	private ArrayList<Lop> _dIterBeforeLops;
	private ArrayList<Lop> _dIterAfterLops;

	private boolean _requiresDIterBeginRecompile = false;
	private boolean _requiresDIterAfterRecompile = false;

	public ArrayList<Hop> getDIterBeforeHops() {
		return _dIterBeForeHops;
	}

	public ArrayList<Hop> getDIterAfterHops() {
		return _dIterAfterHops;
	}

	public void setDIterBeforeHops(ArrayList<Hop> dIterBeforeHops) {
		_dIterBeForeHops = dIterBeforeHops;
	}

	public void setDIterAfterHops(ArrayList<Hop> dIterAfterHops) {
		_dIterAfterHops = dIterAfterHops;
	}

	public ArrayList<Lop> getDIterBeforeLops() {
		return _dIterBeforeLops;
	}

	public ArrayList<Lop> getDIterAfterLops() {
		return _dIterAfterLops;
	}

	public void setDIterBeforeLops(ArrayList<Lop> dIterBeforeLops) {
		_dIterBeforeLops = dIterBeforeLops;
	}

	public void setDIterAfterLops(ArrayList<Lop> dIterAfterLops) {
		_dIterAfterLops = dIterAfterLops;
	}

	public boolean updateDIterBeginRecompilationFlag() {
		return (_requiresDIterBeginRecompile =
				ConfigurationManager.isDynamicRecompilation() && Recompiler.requiresRecompilation(getDIterBeforeHops()));
	}

	public boolean updateDIterAfterRecompilationFlag() {
		return (_requiresDIterAfterRecompile =
				ConfigurationManager.isDynamicRecompilation() && Recompiler.requiresRecompilation(getDIterAfterHops()));
	}

	public boolean requiresDIterBeforeRecompilation() {
		return _requiresDIterBeginRecompile;
	}

	public boolean requiresDIterAfterRecompilation() {
		return _requiresDIterAfterRecompile;
	}

	@Override
	public VariableSet initializeforwardLV(VariableSet activeInPassed) {
		super.initializeforwardLV(activeInPassed);

		DWhileStatement dwstmt = (DWhileStatement) _statements.get(0);

		for (Statement stmt : dwstmt.getDIterBefore()) {
			_read.addVariables(stmt.variablesRead());
			_updated.addVariables(stmt.variablesUpdated());
			_gen.addVariables(stmt.variablesRead());
		}

		_liveOut.addVariables(_updated);
		return _liveOut;
	}

	@Override
	public VariableSet analyze(VariableSet loPassed) {
		// TODO added by czh liveIn
		return super.analyze(loPassed);
	}

}