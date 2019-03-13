package org.apache.sysml.parser;

import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.hops.Hop;
import org.apache.sysml.hops.recompile.Recompiler;
import org.apache.sysml.lops.Lop;

// TODO added by czh: 是否需要常值处理
public class DWhileStatementBlock extends WhileStatementBlock {

	// TODO added by czh 应该是ArrayList<Hop>
	private Hop _dIterBeginHops = null;
	private Hop _dIterAfterHops = null;

	private Lop _dIterBeginLops = null;
	private Lop _dIterAfterLops = null;

	private boolean _requiresDIterBeginRecompile = false;
	private boolean _requiresDIterAfterRecompile = false;

	public Hop getDIterBeginHops() {
		return _dIterBeginHops;
	}

	public Hop getDIterAfterHops() {
		return _dIterAfterHops;
	}

	public void setDIterBeginHops(Hop dIterBeginHops) {
		_dIterBeginHops = dIterBeginHops;
	}

	public void setDIterAfterHops(Hop dIterAfterHops) {
		_dIterAfterHops = dIterAfterHops;
	}

	public Lop getDIterBeginLops() {
		return _dIterBeginLops;
	}

	public Lop getDIterAfterLops() {
		return _dIterAfterLops;
	}

	public void setDIterBeginLops(Lop dIterBeginLops) {
		_dIterBeginLops = dIterBeginLops;
	}

	public void setDIterAfterLops(Lop dIterAfterLops) {
		_dIterAfterLops = dIterAfterLops;
	}

	public boolean updateDIterBeginRecompilationFlag() {
		return (_requiresDIterBeginRecompile =
				ConfigurationManager.isDynamicRecompilation() && Recompiler.requiresRecompilation(getDIterBeginHops()));
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

}