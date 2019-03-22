package org.apache.sysml.parser;

import java.util.HashMap;

public class DWhileStatementBlock extends WhileStatementBlock {

	@Override
	public VariableSet initializeforwardLV(VariableSet activeInPassed) {
		DWhileStatement dwst = (DWhileStatement) _statements.get(0);

		if (_statements.size() > 1) {
			throw new LanguageException(_statements.get(0).printErrorLocation()
					+ "DWhileStatementBlock should have only 1 statement (dwhile statement)");
		}

		_read = new VariableSet();
		_read.addVariables(dwst.getConditionalPredicate().variablesRead());
		_updated.addVariables(dwst.getConditionalPredicate().variablesUpdated());

		_gen = new VariableSet();
		_gen.addVariables(dwst.getConditionalPredicate().variablesRead());

		VariableSet current = new VariableSet();
		current.addVariables(activeInPassed);

		for (StatementBlock sb : dwst.getDIterInit()) {
			current = forwardLVOfStatementBlock(sb, current);
		}

		for (StatementBlock sb : dwst.getDIterBefore()) {
			current = forwardLVOfStatementBlock(sb, current);
		}

		for (StatementBlock sb : dwst.getBody()) {
			current = forwardLVOfStatementBlock(sb, current);
		}

		for (StatementBlock sb : dwst.getDIterAfter()) {
			current = forwardLVOfStatementBlock(sb, current);
		}

		for (String varName : _updated.getVariableNames()) {
			if (!activeInPassed.containsVariable(varName)) {
				_warnSet.addVariable(varName, _updated.getVariable(varName));
			}
		}

		_liveOut = new VariableSet();
		_liveOut.addVariables(current);
		_liveOut.addVariables(_updated);
		return _liveOut;
	}

	@Override
	public VariableSet initializebackwardLV(VariableSet loPassed) {
		DWhileStatement dwst = (DWhileStatement) _statements.get(0);

		VariableSet lo = new VariableSet();
		lo.addVariables(loPassed);

		// init
		for (int i = dwst.getDIterInit().size() - 1; i >= 0; i--) {
			lo = dwst.getDIterInitByIndex(i).analyze(lo);
		}

		// before
		for (int i = dwst.getDIterBefore().size() - 1; i >= 0; i--) {
			lo = dwst.getDIterBeforeByIndex(i).analyze(lo);
		}

		// body
		for (int i = dwst.getBody().size() - 1; i >= 0; i--) {
			lo = dwst.getBody().get(i).analyze(lo);
		}

		// after
		for (int i = dwst.getDIterAfter().size() - 1; i >= 0; i--) {
			lo = dwst.getDIterAfterByIndex(i).analyze(lo);
		}

		VariableSet loReturn = new VariableSet();
		loReturn.addVariables(lo);
		return loReturn;
	}

	@Override
	public VariableSet analyze(VariableSet loPassed) {
		DWhileStatement dwst = (DWhileStatement) _statements.get(0);
		VariableSet predVars = new VariableSet();
		predVars.addVariables(dwst.getConditionalPredicate().variablesRead());
		predVars.addVariables(dwst.getConditionalPredicate().variablesUpdated());

		VariableSet candidateLO = new VariableSet();
		candidateLO.addVariables(loPassed);
		candidateLO.addVariables(_gen);
		candidateLO.addVariables(predVars);

		VariableSet origLiveOut = new VariableSet();
		origLiveOut.addVariables(_liveOut);
		origLiveOut.addVariables(predVars);
		origLiveOut.addVariables(_gen);

		_liveOut = new VariableSet();
		for (String name : candidateLO.getVariableNames()) {
			if (origLiveOut.containsVariable(name)) {
				_liveOut.addVariable(name, candidateLO.getVariable(name));
			}
		}

		initializebackwardLV(_liveOut);

		VariableSet finalWarnSet = new VariableSet();
		for (String varName : _warnSet.getVariableNames()) {
			if (_liveOut.containsVariable(varName)) {
				finalWarnSet.addVariable(varName, _warnSet.getVariable(varName));
			}
		}
		_warnSet = finalWarnSet;

		for (String varName : _warnSet.getVariableNames()) {
			LOG.warn(_warnSet.getVariable(varName).printWarningLocation() + "Initialization of " + varName
					+ " depends on dwhile execution");
		}

		_liveIn = new VariableSet();
		_liveIn.addVariables(_liveOut);
		_liveIn.addVariables(_gen);

		VariableSet liveInReturn = new VariableSet();
		liveInReturn.addVariables(_liveIn);

		return liveInReturn;
	}

	@Override
	public VariableSet validate(DMLProgram dmlProg, VariableSet ids, HashMap<String, ConstIdentifier> constVars, boolean conditional) {
		if (_statements.size() > 1) {
			raiseValidateError("DWhileStatementBlock should have only 1 statement (while statement)", conditional);
		}

		DWhileStatement dwst = (DWhileStatement) _statements.get(0);
		ConditionalPredicate predicate = dwst.getConditionalPredicate();
		StatementBlock init = dwst.getDIterInitByIndex(0);
		StatementBlock before = dwst.getDIterBeforeByIndex(0);
		StatementBlock body = dwst.getBody().get(0);
		StatementBlock after = dwst.getDIterAfterByIndex(0);

		_dmlProg = dmlProg;

		VariableSet origVarsBeforeBody = new VariableSet();
		for (String key : ids.getVariableNames()) {
			DataIdentifier origId = ids.getVariable(key);
			DataIdentifier copyId = new DataIdentifier(origId);
			origVarsBeforeBody.addVariable(key, copyId);
		}

		//////////////////////////////////////////////////////////////////////////////
		// FIRST PASS: process the predicate / statement blocks in the body of the for statement
		///////////////////////////////////////////////////////////////////////////////

		for (String var : _updated.getVariableNames()) {
			if (constVars.containsKey(var)) {
				constVars.remove(var);
			}
		}

		predicate.getPredicate().validateExpression(ids.getVariables(), constVars, conditional);

		ids = init.validate(dmlProg, ids, constVars, true);
		ids = before.validate(dmlProg, ids, constVars, true);
		ids = body.validate(dmlProg, ids, constVars, true);
		ids = after.validate(dmlProg, ids, constVars, true);
		constVars = body.getConstOut();

		_constVarsIn.putAll(body.getConstIn());
		_constVarsOut.putAll(body.getConstOut());

		boolean revalidationRequired = false;
		for (String key : _updated.getVariableNames()) {
			DataIdentifier startVersion = origVarsBeforeBody.getVariable(key);
			DataIdentifier endVersion = ids.getVariable(key);

			if (startVersion != null && endVersion != null) {
				if (!startVersion.getOutput().getDataType().equals(endVersion.getOutput().getDataType())) {
					raiseValidateError("DWhileStatementBlock has unsupported conditional data type change of " +
							"variable '" + key + "' in loop body.", conditional);
				}

				long startVersionDim1 = (startVersion instanceof IndexedIdentifier) ?
						((IndexedIdentifier) startVersion).getOrigDim1() : startVersion.getDim1();
				long endVersionDim1 = (endVersion instanceof IndexedIdentifier) ?
						((IndexedIdentifier) endVersion).getOrigDim1() : endVersion.getDim1();
				long startVersionDim2 = (startVersion instanceof IndexedIdentifier) ?
						((IndexedIdentifier) startVersion).getOrigDim2() : startVersion.getDim2();
				long endVersionDim2 = (endVersion instanceof IndexedIdentifier) ?
						((IndexedIdentifier) endVersion).getOrigDim2() : endVersion.getDim2();

				boolean sizeUnchanged = ((startVersionDim1 == endVersionDim1) && (startVersionDim2 == endVersionDim2));

				boolean nnzUnchanged = false;

				if (!sizeUnchanged || !nnzUnchanged) {
					revalidationRequired = true;
					DataIdentifier recVersion = new DataIdentifier(endVersion);
					if (!sizeUnchanged)
						recVersion.setDimensions(-1, -1);
					if (!nnzUnchanged)
						recVersion.setNnz(-1);
					origVarsBeforeBody.addVariable(key, recVersion);
				}
			}
		}

		if (revalidationRequired) {
			_dmlProg = dmlProg;

			ids = origVarsBeforeBody;

			//////////////////////////////////////////////////////////////////////////////
			// SECOND PASS: process the predicate / statement blocks in the body of the for statement
			///////////////////////////////////////////////////////////////////////////////

			for (String var : _updated.getVariableNames()) {
				if (constVars.containsKey(var)) {
					constVars.remove(var);
				}
			}

			predicate.getPredicate().validateExpression(ids.getVariables(), constVars, conditional);

			ids = init.validate(dmlProg, ids, constVars, true);
			ids = before.validate(dmlProg, ids, constVars, true);
			ids = body.validate(dmlProg, ids, constVars, true);
			ids = after.validate(dmlProg, ids, constVars, true);

			_constVarsIn.putAll(body.getConstIn());
			_constVarsOut.putAll(body.getConstOut());
		}

		// 为hop输出旧值设置统计信息
		// TODO added by czh 可能不完善
		for (DataIdentifier var : _updated.getVariables().values()) {
			if (var instanceof PreDataIdentifier) {
				DataIdentifier originVar = _updated.getVariable(((PreDataIdentifier) var).getOriginName());
				var.setProperties(originVar);
			}
		}

		return ids;
	}

	private VariableSet forwardLVOfStatementBlock(StatementBlock sb, VariableSet variableSet) {
		VariableSet current = sb.initializeforwardLV(variableSet);

		for (String varName : sb._gen.getVariableNames()) {
			if (!_kill.getVariableNames().contains(varName)) {
				_gen.addVariable(varName, sb._gen.getVariable(varName));
			}
		}

		_read.addVariables(sb._read);
		_updated.addVariables(sb._updated);

		if (!(sb instanceof WhileStatementBlock) && !(sb instanceof ForStatementBlock)) {
			_kill.addVariables(sb._kill);
		}

		return current;
	}

}