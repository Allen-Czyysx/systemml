package org.apache.sysml.parser;

import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.lops.Lop;

public class DWhileStatementBlock extends WhileStatementBlock {

	// TODO added by czh 应该是ArrayList<Hop>
	private Hop _dIterBeForeHops;
	private Hop _dIterAfterHops;

	private Lop _dIterBeforeLops;
	private Lop _dIterAfterLops;

	private boolean _requiresDIterBeginRecompile = false;
	private boolean _requiresDIterAfterRecompile = false;

	public Hop getDIterBeforeHops() {
		return _dIterBeForeHops;
	}

	public Hop getDIterAfterHops() {
		return _dIterAfterHops;
	}

	public void setDIterBeforeHops(Hop dIterBeforeHops) {
		_dIterBeForeHops = dIterBeforeHops;
	}

	public void setDIterAfterHops(Hop dIterAfterHops) {
		_dIterAfterHops = dIterAfterHops;
	}

	public Lop getDIterBeforeLops() {
		return _dIterBeforeLops;
	}

	public Lop getDIterAfterLops() {
		return _dIterAfterLops;
	}

	public void setDIterBeforeLops(Lop dIterBeforeLops) {
		_dIterBeforeLops = dIterBeforeLops;
	}

	public void setDIterAfterLops(Lop dIterAfterLops) {
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

	public boolean requiresDIterBeginRecompilation() {
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