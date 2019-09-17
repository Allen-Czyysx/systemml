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

package org.apache.sysml.parser.dml;

import java.util.*;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.sysml.conf.CompilerConfig.ConfigType;
import org.apache.sysml.conf.ConfigurationManager;
import org.apache.sysml.parser.*;
import org.apache.sysml.parser.common.CommonSyntacticValidator;
import org.apache.sysml.parser.common.CustomErrorListener;
import org.apache.sysml.parser.common.ExpressionInfo;
import org.apache.sysml.parser.common.StatementInfo;
import org.apache.sysml.parser.dml.DmlParser.AccumulatorAssignmentStatementContext;
import org.apache.sysml.parser.dml.DmlParser.AddSubExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.AssignmentStatementContext;
import org.apache.sysml.parser.dml.DmlParser.AtomicExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.BooleanAndExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.BooleanNotExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.BooleanOrExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.BuiltinFunctionExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.CommandlineParamExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.CommandlinePositionExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ConstDoubleIdExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ConstFalseExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ConstIntIdExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ConstStringIdExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ConstTrueExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.DataIdExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.DataIdentifierContext;
import org.apache.sysml.parser.dml.DmlParser.DWhileStatementContext;
import org.apache.sysml.parser.dml.DmlParser.ExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ExternalFunctionDefExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ForStatementContext;
import org.apache.sysml.parser.dml.DmlParser.FunctionCallAssignmentStatementContext;
import org.apache.sysml.parser.dml.DmlParser.FunctionCallMultiAssignmentStatementContext;
import org.apache.sysml.parser.dml.DmlParser.FunctionStatementContext;
import org.apache.sysml.parser.dml.DmlParser.IfStatementContext;
import org.apache.sysml.parser.dml.DmlParser.IfdefAssignmentStatementContext;
import org.apache.sysml.parser.dml.DmlParser.ImportStatementContext;
import org.apache.sysml.parser.dml.DmlParser.IndexedExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.InternalFunctionDefExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.IterablePredicateColonExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.IterablePredicateSeqExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.MatrixDataTypeCheckContext;
import org.apache.sysml.parser.dml.DmlParser.MatrixMulExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.SpecialMatrixMulExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.Ml_typeContext;
import org.apache.sysml.parser.dml.DmlParser.ModIntDivExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.MultDivExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.MultiIdExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ParForStatementContext;
import org.apache.sysml.parser.dml.DmlParser.ParameterizedExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.PathStatementContext;
import org.apache.sysml.parser.dml.DmlParser.PowerExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ProgramrootContext;
import org.apache.sysml.parser.dml.DmlParser.RelationalExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.SimpleDataIdentifierExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.StatementContext;
import org.apache.sysml.parser.dml.DmlParser.StrictParameterizedExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.StrictParameterizedKeyValueStringContext;
import org.apache.sysml.parser.dml.DmlParser.TypedArgAssignContext;
import org.apache.sysml.parser.dml.DmlParser.TypedArgNoAssignContext;
import org.apache.sysml.parser.dml.DmlParser.UnaryExpressionContext;
import org.apache.sysml.parser.dml.DmlParser.ValueTypeContext;
import org.apache.sysml.parser.dml.DmlParser.WhileStatementContext;
import org.apache.sysml.runtime.util.UtilFunctions;

import static org.apache.sysml.api.DMLScript.USE_LOCAL_SPARK_CONFIG;
import static org.apache.sysml.hops.OptimizerUtils.DEFAULT_BLOCKSIZE;


public class DmlSyntacticValidator extends CommonSyntacticValidator implements DmlListener {

	public DmlSyntacticValidator(CustomErrorListener errorListener, Map<String, String> argVals,
								 String sourceNamespace, Set<String> prepFunctions) {
		super(errorListener, argVals, sourceNamespace, prepFunctions);
	}

	@Override
	public String namespaceResolutionOp() {
		return "::";
	}

	@Override
	public String trueStringLiteral() {
		return "TRUE";
	}

	@Override
	public String falseStringLiteral() {
		return "FALSE";
	}

	protected ArrayList<ParameterExpression> getParameterExpressionList(List<ParameterizedExpressionContext> paramExprs) {
		ArrayList<ParameterExpression> retVal = new ArrayList<>();
		for (ParameterizedExpressionContext ctx : paramExprs) {
			String paramName = null;
			if (ctx.paramName != null && ctx.paramName.getText() != null && !ctx.paramName.getText().isEmpty()) {
				paramName = ctx.paramName.getText();
			}
			ParameterExpression myArg = new ParameterExpression(paramName, ctx.paramVal.info.expr);
			retVal.add(myArg);
		}
		return retVal;
	}

	@Override
	public void enterEveryRule(ParserRuleContext arg0) {
		if (arg0 instanceof StatementContext) {
			if (((StatementContext) arg0).info == null) {
				((StatementContext) arg0).info = new StatementInfo();
			}
		}
		if (arg0 instanceof FunctionStatementContext) {
			if (((FunctionStatementContext) arg0).info == null) {
				((FunctionStatementContext) arg0).info = new StatementInfo();
			}
		}
		if (arg0 instanceof ExpressionContext) {
			if (((ExpressionContext) arg0).info == null) {
				((ExpressionContext) arg0).info = new ExpressionInfo();
			}
		}
		if (arg0 instanceof DataIdentifierContext) {
			if (((DataIdentifierContext) arg0).dataInfo == null) {
				((DataIdentifierContext) arg0).dataInfo = new ExpressionInfo();
			}
		}
	}

	// -----------------------------------------------------------------
	// 			Binary, Unary & Relational Expressions
	// -----------------------------------------------------------------

	// For now do no type checking, let validation handle it.
	// This way parser doesn't have to open metadata file
	@Override
	public void exitAddSubExpression(AddSubExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitModIntDivExpression(ModIntDivExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitUnaryExpression(UnaryExpressionContext ctx) {
		unaryExpressionHelper(ctx, ctx.left.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitMultDivExpression(MultDivExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitPowerExpression(PowerExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitMatrixMulExpression(MatrixMulExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitSpecialMatrixMulExpression(SpecialMatrixMulExpressionContext ctx) {
		binaryExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitRelationalExpression(RelationalExpressionContext ctx) {
		relationalExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitBooleanAndExpression(BooleanAndExpressionContext ctx) {
		booleanExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitBooleanOrExpression(BooleanOrExpressionContext ctx) {
		booleanExpressionHelper(ctx, ctx.left.info, ctx.right.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitBooleanNotExpression(BooleanNotExpressionContext ctx) {
		unaryBooleanExpressionHelper(ctx, ctx.left.info, ctx.info, ctx.op.getText());
	}

	@Override
	public void exitAtomicExpression(AtomicExpressionContext ctx) {
		ctx.info.expr = ctx.left.info.expr;
		setFileLineColumn(ctx.info.expr, ctx);
	}

	// -----------------------------------------------------------------
	// 			Constant Expressions
	// -----------------------------------------------------------------

	@Override
	public void exitConstFalseExpression(ConstFalseExpressionContext ctx) {
		booleanIdentifierHelper(ctx, false, ctx.info);
	}

	@Override
	public void exitConstTrueExpression(ConstTrueExpressionContext ctx) {
		booleanIdentifierHelper(ctx, true, ctx.info);
	}

	@Override
	public void exitConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {
		constDoubleIdExpressionHelper(ctx, ctx.info);
	}

	@Override
	public void exitConstIntIdExpression(ConstIntIdExpressionContext ctx) {
		constIntIdExpressionHelper(ctx, ctx.info);
	}

	@Override
	public void exitConstStringIdExpression(ConstStringIdExpressionContext ctx) {
		constStringIdExpressionHelper(ctx, ctx.info);
	}


	// -----------------------------------------------------------------
	//          Identifier Based Expressions
	// -----------------------------------------------------------------

	@Override
	public void exitDataIdExpression(DataIdExpressionContext ctx) {
		exitDataIdExpressionHelper(ctx, ctx.info, ctx.dataIdentifier().dataInfo);
	}

	@Override
	public void exitSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {
		// This is either a function, or variable with namespace
		// By default, it assigns to a data type
		ctx.dataInfo.expr = new DataIdentifier(ctx.getText());
		setFileLineColumn(ctx.dataInfo.expr, ctx);
	}

	/**
	 * DML uses 1-based indexing.;
	 *
	 * @param ctx the parse tree
	 */
	@Override
	public void exitIndexedExpression(IndexedExpressionContext ctx) {
		boolean isRowLower = (ctx.rowLower != null && !ctx.rowLower.isEmpty() && (ctx.rowLower.info.expr != null));
		boolean isRowUpper = (ctx.rowUpper != null && !ctx.rowUpper.isEmpty() && (ctx.rowUpper.info.expr != null));
		boolean isColLower = (ctx.colLower != null && !ctx.colLower.isEmpty() && (ctx.colLower.info.expr != null));
		boolean isColUpper = (ctx.colUpper != null && !ctx.colUpper.isEmpty() && (ctx.colUpper.info.expr != null));
		ExpressionInfo rowLower = isRowLower ? ctx.rowLower.info : null;
		ExpressionInfo rowUpper = isRowUpper ? ctx.rowUpper.info : null;
		ExpressionInfo colLower = isColLower ? ctx.colLower.info : null;
		ExpressionInfo colUpper = isColUpper ? ctx.colUpper.info : null;

		ctx.dataInfo.expr = new IndexedIdentifier(ctx.name.getText(), false, false);
		setFileLineColumn(ctx.dataInfo.expr, ctx);

		try {
			ArrayList<ArrayList<Expression>> exprList = new ArrayList<>();

			ArrayList<Expression> rowIndices = new ArrayList<>();
			ArrayList<Expression> colIndices = new ArrayList<>();


			if (!isRowLower && !isRowUpper) {
				// both not set
				rowIndices.add(null);
				rowIndices.add(null);
			} else if (isRowLower && isRowUpper) {
				// both set
				rowIndices.add(rowLower.expr);
				rowIndices.add(rowUpper.expr);
			} else if (isRowLower && !isRowUpper) {
				// only row set
				rowIndices.add(rowLower.expr);
			} else {
				notifyErrorListeners("incorrect index expression for row", ctx.start);
				return;
			}

			if (!isColLower && !isColUpper) {
				// both not set
				colIndices.add(null);
				colIndices.add(null);
			} else if (isColLower && isColUpper) {
				colIndices.add(colLower.expr);
				colIndices.add(colUpper.expr);
			} else if (isColLower && !isColUpper) {
				colIndices.add(colLower.expr);
			} else {
				notifyErrorListeners("incorrect index expression for column", ctx.start);
				return;
			}
			exprList.add(rowIndices);
			exprList.add(colIndices);
			((IndexedIdentifier) ctx.dataInfo.expr).setIndices(exprList);
		} catch (Exception e) {
			notifyErrorListeners("cannot set the indices", ctx.start);
			return;
		}
	}


	// -----------------------------------------------------------------
	//          Command line parameters (begin with a '$')
	// -----------------------------------------------------------------

	@Override
	public void exitCommandlineParamExpression(CommandlineParamExpressionContext ctx) {
		handleCommandlineArgumentExpression(ctx);
	}

	@Override
	public void exitCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {
		handleCommandlineArgumentExpression(ctx);
	}

	private void handleCommandlineArgumentExpression(DataIdentifierContext ctx) {
		String varName = ctx.getText().trim();
		fillExpressionInfoCommandLineParameters(ctx, varName, ctx.dataInfo);

		if (ctx.dataInfo.expr == null) {
			if (!(ctx.parent instanceof IfdefAssignmentStatementContext)) {
				String msg = "The parameter " + varName + " either needs to be passed "
						+ "through commandline or initialized to default value.";
				if (ConfigurationManager.getCompilerConfigFlag(ConfigType.IGNORE_UNSPECIFIED_ARGS)) {
					ctx.dataInfo.expr = getConstIdFromString(ctx, " ");
					if (!ConfigurationManager.getCompilerConfigFlag(ConfigType.MLCONTEXT)) {
						raiseWarning(msg, ctx.start);
					}
				} else {
					notifyErrorListeners(msg, ctx.start);
				}
			}
		}
	}


	// -----------------------------------------------------------------
	// 			"source" statement
	// -----------------------------------------------------------------

	@Override
	public void exitImportStatement(ImportStatementContext ctx) {
		String filePath = getWorkingFilePath(UtilFunctions.unquote(ctx.filePath.getText()));
		String namespace = getNamespaceSafe(ctx.namespace);

		validateNamespace(namespace, filePath, ctx);
		String scriptID = DMLProgram.constructFunctionKey(namespace, filePath);

		DMLProgram prog = null;
		if (!_f2NS.get().containsKey(scriptID)) {
			_f2NS.get().put(scriptID, namespace);
			try {
				prog = (new DMLParserWrapper()).doParse(filePath,
						_tScripts.get().get(filePath), getQualifiedNamespace(namespace), argVals);
			} catch (ParseException e) {
				notifyErrorListeners(e.getMessage(), ctx.start);
				return;
			}
			if (prog == null) {
				notifyErrorListeners("One or more errors found during importing a program from file " + filePath,
						ctx.start);
				return;
			}
			setupContextInfo(ctx.info, namespace, filePath, ctx.filePath.getText(), prog);
		} else {
			// Skip redundant parsing (to prevent potential infinite recursion) and
			// create empty program for this context to allow processing to continue.
			prog = new DMLProgram();
			setupContextInfo(ctx.info, namespace, filePath, ctx.filePath.getText(), prog);
		}
	}

	// -----------------------------------------------------------------
	// 			Assignment Statement
	// -----------------------------------------------------------------

	@Override
	public void exitAssignmentStatement(AssignmentStatementContext ctx) {
		if (ctx.targetList == null) {
			notifyErrorListeners("incorrect parsing for assignment", ctx.start);
			return;
		}
		exitAssignmentStatementHelper(ctx, ctx.targetList.getText(), ctx.targetList.dataInfo, ctx.targetList.start,
				ctx.source.info, ctx.info);
	}


	// -----------------------------------------------------------------
	// 			Control Statements - Guards & Loops
	// -----------------------------------------------------------------

	@Override
	public ConvertedDMLSyntax convertToDMLSyntax(ParserRuleContext ctx, String namespace, String functionName,
												 ArrayList<ParameterExpression> paramExpression, Token fnName) {
		return new ConvertedDMLSyntax(namespace, functionName, paramExpression);
	}


	@Override
	protected Expression handleLanguageSpecificFunction(ParserRuleContext ctx, String functionName,
														ArrayList<ParameterExpression> paramExpressions) {
		return null;
	}


	@Override
	public void exitFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {

		Set<String> printStatements = new HashSet<>();
		printStatements.add("print");
		printStatements.add("stop");
		printStatements.add("assert");

		Set<String> outputStatements = new HashSet<>();
		outputStatements.add("write");

		String[] fnNames = getQualifiedNames(ctx.name.getText());
		if (fnNames == null) {
			String errorMsg = "incorrect function name (only namespace " + namespaceResolutionOp() + " functionName " +
					"allowed. Hint: If you are trying to use builtin functions, you can skip the namespace)";
			notifyErrorListeners(errorMsg, ctx.name);
			return;
		}
		String namespace = fnNames[0];
		String functionName = fnNames[1];
		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);

		castAsScalarDeprecationCheck(functionName, ctx);

		boolean hasLHS = ctx.targetList != null;
		functionCallAssignmentStatementHelper(ctx, printStatements, outputStatements, hasLHS ?
						ctx.targetList.dataInfo.expr : null, ctx.info, ctx.name,
				hasLHS ? ctx.targetList.start : null, namespace, functionName, paramExpression, hasLHS);
	}

	// TODO: remove this when castAsScalar has been removed from DML/PYDML
	private void castAsScalarDeprecationCheck(String functionName, ParserRuleContext ctx) {
		if ("castAsScalar".equalsIgnoreCase(functionName)) {
			raiseWarning("castAsScalar() has been deprecated. Please use as.scalar().", ctx.start);
		}
	}

	@Override
	public void exitBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {
		// Double verification: verify passed function name is a (non-parameterized) built-in function.
		String[] names = getQualifiedNames(ctx.name.getText());
		if (names == null) {
			notifyErrorListeners("incorrect function name (only namespace " + namespaceResolutionOp() + " " +
							"functionName" +
							" allowed. Hint: If you are trying to use builtin functions, you can skip the namespace)",
					ctx.name);
			return;
		}
		String namespace = names[0];
		String functionName = names[1];

		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);
		castAsScalarDeprecationCheck(functionName, ctx);

		ConvertedDMLSyntax convertedSyntax = convertToDMLSyntax(ctx, namespace, functionName, paramExpression,
				ctx.name);
		if (convertedSyntax == null) {
			return;
		} else {
			functionName = convertedSyntax.functionName;
			paramExpression = convertedSyntax.paramExpression;
		}

		// handle built-in functions
		ctx.info.expr = buildForBuiltInFunction(ctx, functionName, paramExpression);
		if (ctx.info.expr != null)
			return;

		// handle user-defined functions
		ctx.info.expr = createFunctionCall(ctx, namespace, functionName, paramExpression);
	}


	@Override
	public void exitFunctionCallMultiAssignmentStatement(
			FunctionCallMultiAssignmentStatementContext ctx) {
		String[] names = getQualifiedNames(ctx.name.getText());
		if (names == null) {
			notifyErrorListeners("incorrect function name (only namespace.functionName allowed. Hint: If you are " +
					"trying to use builtin functions, you can skip the namespace)", ctx.name);
			return;
		}
		String namespace = names[0];
		String functionName = names[1];

		ArrayList<ParameterExpression> paramExpression = getParameterExpressionList(ctx.paramExprs);
		ConvertedDMLSyntax convertedSyntax = convertToDMLSyntax(ctx, namespace, functionName, paramExpression,
				ctx.name);
		if (convertedSyntax == null) {
			return;
		} else {
			namespace = convertedSyntax.namespace;
			functionName = convertedSyntax.functionName;
			paramExpression = convertedSyntax.paramExpression;
		}

		FunctionCallIdentifier functCall = new FunctionCallIdentifier(paramExpression);
		functCall.setFunctionName(functionName);
		functCall.setFunctionNamespace(namespace);

		final ArrayList<DataIdentifier> targetList = new ArrayList<>();
		for (DataIdentifierContext dataCtx : ctx.targetList) {
			if (dataCtx.dataInfo.expr instanceof DataIdentifier) {
				targetList.add((DataIdentifier) dataCtx.dataInfo.expr);
			} else {
				notifyErrorListeners("incorrect type for variable ", dataCtx.start);
				return;
			}
		}

		if (namespace.equals(DMLProgram.DEFAULT_NAMESPACE)) {
			Expression e = buildForBuiltInFunction(ctx, functionName, paramExpression);
			if (e != null) {
				setMultiAssignmentStatement(targetList, e, ctx, ctx.info);
				return;
			}
		}

		// Override default namespace for imported non-built-in function
		String inferNamespace =
				(sourceNamespace != null && sourceNamespace.length() > 0 && DMLProgram.DEFAULT_NAMESPACE.equals(namespace)) ? sourceNamespace : namespace;
		functCall.setFunctionNamespace(inferNamespace);

		setMultiAssignmentStatement(targetList, functCall, ctx, ctx.info);
	}


	// -----------------------------------------------------------------
	// 			Control Statements - Guards & Loops
	// -----------------------------------------------------------------

	private static StatementBlock getStatementBlock(Statement current) {
		return ParserWrapper.getStatementBlock(current);
	}

	@Override
	public void exitIfStatement(IfStatementContext ctx) {
		IfStatement ifStmt = new IfStatement();
		ConditionalPredicate predicate = new ConditionalPredicate(ctx.predicate.info.expr);
		ifStmt.setConditionalPredicate(predicate);
		ifStmt.setCtxValuesAndFilename(ctx, currentFile);

		if (ctx.ifBody.size() > 0) {
			for (StatementContext stmtCtx : ctx.ifBody) {
				ifStmt.addStatementBlockIfBody(getStatementBlock(stmtCtx.info.stmt));
			}
			ifStmt.mergeStatementBlocksIfBody();
		}

		if (ctx.elseBody.size() > 0) {
			for (StatementContext stmtCtx : ctx.elseBody) {
				ifStmt.addStatementBlockElseBody(getStatementBlock(stmtCtx.info.stmt));
			}
			ifStmt.mergeStatementBlocksElseBody();
		}

		ctx.info.stmt = ifStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitWhileStatement(WhileStatementContext ctx) {
		WhileStatement whileStmt = new WhileStatement();
		ConditionalPredicate predicate = new ConditionalPredicate(ctx.predicate.info.expr);
		whileStmt.setPredicate(predicate);
		whileStmt.setCtxValuesAndFilename(ctx, currentFile);

		if (ctx.body.size() > 0) {
			for (StatementContext stmtCtx : ctx.body) {
				whileStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			whileStmt.mergeStatementBlocks();
		}

		ctx.info.stmt = whileStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitDWhileStatement(DWhileStatementContext ctx) {
		DWhileStatement dwst = new DWhileStatement();
		dwst.setCtxValuesAndFilename(ctx, currentFile);
		Expression predicateExp = ctx.predicate.info.expr;

		// 获得dVarName
		String names = ctx.dVarList.getText();
		names = names.substring(1, names.length() - 1);
		String[] dVarNames = names.split(",");
		dwst.setDVarNames(dVarNames);

		ArrayList<StatementBlock> init = new ArrayList<>();
		ArrayList<StatementBlock> before = new ArrayList<>();
		ArrayList<StatementBlock> after = new ArrayList<>();

		// dwhileCount = 0
		DataIdentifier dwhileCount = new DataIdentifier(DWhileStatement.getDwhileCountName());
		AssignmentStatement initDwhileCount = new AssignmentStatement(ctx, dwhileCount, new IntIdentifier(0));
		init.add(getStatementBlock(initDwhileCount));

		for (String dVarName : dVarNames) {
			String preVarName = DWhileStatement.getPreVarName(dVarName);
			DataIdentifier var = new DataIdentifier(dVarName);
			PreDataIdentifier preVar = new PreDataIdentifier(preVarName, dVarName);
			DataIdentifier useDelta = new DataIdentifier(DWhileStatement.getVarUseDeltaName(dVarName));
			DataIdentifier select = new DataIdentifier(DWhileStatement.getSelectName(dVarName));
			DataIdentifier selectNum = new DataIdentifier(DWhileStatement.getSelectNumName(dVarName));
			DataIdentifier deltaVar = new DataIdentifier(DWhileStatement.getDeltaName(dVarName));
			DataIdentifier selectBlockNum = new DataIdentifier(DWhileStatement.getSelectBlockNumName(dVarName));
			DataIdentifier repartitionBlockNum =
					new DataIdentifier(DWhileStatement.getRepartitionBlockNumName(dVarName));
			DataIdentifier repartitionPreBlockNum =
					new DataIdentifier(DWhileStatement.getRepartitionPreBlockNumName(dVarName));

			DataIdentifier useFilter = new DataIdentifier(DWhileStatement.getUseFilterName(dVarName));
			DataIdentifier useRepartition = new DataIdentifier(DWhileStatement.getUseRepartitionName(dVarName));
			DataIdentifier useRepartitionCount =
					new DataIdentifier(DWhileStatement.getUseRepartitionCountName(dVarName));
			DataIdentifier isDetect = new DataIdentifier(DWhileStatement.getIsDetectName(dVarName));


//			// 循环条件: 添加 & selectNum > DEFAULT_BLOCKSIZE
//			// TODO added by czh 多个怎么办
//			RelationalExpression detectSelectNum = new RelationalExpression(Expression.RelationalOp.GREATER, dwst);
//			detectSelectNum.setLeft(selectNum);
//			detectSelectNum.setRight(new IntIdentifier(DEFAULT_BLOCKSIZE));
//			BooleanExpression newPredicateExp = new BooleanExpression(Expression.BooleanOp.LOGICALAND, dwst);
//			newPredicateExp.setLeft(predicateExp);
//			newPredicateExp.setRight(detectSelectNum);
//			predicateExp = newPredicateExp;


			// Init: 创建useDelta, useDeltaCount, ...
			// useDelta 标记下次迭代是否使用增量
			AssignmentStatement disableUseDelta =
					new AssignmentStatement(ctx, useDelta, new BooleanIdentifier(false));
			init.add(getStatementBlock(disableUseDelta));

			// selectBlockNum = nrow(var) / DEFAULT_BLOCKSIZE
			BuiltinFunctionExpression rowNum = new BuiltinFunctionExpression(
					ctx, Expression.BuiltinFunctionOp.NROW, new Expression[]{var}, currentFile);
			BinaryExpression maxBlockNum = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
			maxBlockNum.setLeft(rowNum);
			maxBlockNum.setRight(new IntIdentifier(ctx, DEFAULT_BLOCKSIZE, currentFile));
			AssignmentStatement initBlockNum = new AssignmentStatement(ctx, selectBlockNum, maxBlockNum);
			init.add(getStatementBlock(initBlockNum));

			// repartitionPreBlockNum = nrow(var) / DEFAULT_BLOCKSIZE
			AssignmentStatement initRepartitionPreBlockNum =
					new AssignmentStatement(ctx, repartitionPreBlockNum, maxBlockNum);
			init.add(getStatementBlock(initRepartitionPreBlockNum));

//			// repartitionBlockNum = nrow(var) / DEFAULT_BLOCKSIZE
//			AssignmentStatement initRepartitionBlockNum =
//					new AssignmentStatement(ctx, repartitionBlockNum, maxBlockNum);
//			init.add(getStatementBlock(initRepartitionBlockNum));

			// useFilter = true
			AssignmentStatement initUseFilter =
					new AssignmentStatement(ctx, useFilter, new BooleanIdentifier(true));
			init.add(getStatementBlock(initUseFilter));

			// useRepartition = false
			AssignmentStatement disableRepartition =
					new AssignmentStatement(ctx, useRepartition, new BooleanIdentifier(false));
			init.add(getStatementBlock(disableRepartition));

			// useRepartitionCount = 1
			AssignmentStatement initUseRepartitionCount =
					new AssignmentStatement(ctx, useRepartitionCount, new IntIdentifier(1));
			init.add(getStatementBlock(initUseRepartitionCount));

			// delta = var
			AssignmentStatement initDelta = new AssignmentStatement(ctx, deltaVar, var);
			init.add(getStatementBlock(initDelta));

			// isDetect = false
			AssignmentStatement initIsDetect = new AssignmentStatement(ctx, isDetect, new BooleanIdentifier(false));
			init.add(getStatementBlock(initIsDetect));


			// Before: 记录input旧值
			AssignmentStatement assignPreDVar = new AssignmentStatement(ctx, preVar, var);
			before.add(getStatementBlock(assignPreDVar));


			// After: 计算input增量, 为下一次迭代做准备
			// TODO added by czh 要求dvar必须是列向量或高瘦矩阵

			// 重置
			after.add(getStatementBlock(disableRepartition));
			AssignmentStatement disableFilter =
					new AssignmentStatement(ctx, useFilter, new BooleanIdentifier(false));
			after.add(getStatementBlock(disableFilter));
			after.add(getStatementBlock(disableUseDelta));

			// select = selectRow(var / preVar - 1, ratio)
			BinaryExpression divPre = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
			divPre.setLeft(var);
			divPre.setRight(preVar);
			BinaryExpression related = new BinaryExpression(Expression.BinaryOp.MINUS, dwst);
			related.setLeft(divPre);
			related.setRight(new IntIdentifier(1));
			RelationalExpression selectVar = new RelationalExpression(Expression.RelationalOp.SELECTROW, dwst);
			double ratio = Double.parseDouble(ctx.ratio.getText());
			selectVar.setLeft(related);
			selectVar.setRight(new DoubleIdentifier(ratio));
			AssignmentStatement assignSelect = new AssignmentStatement(ctx, select, selectVar);
			after.add(getStatementBlock(assignSelect));

			// 判断是否检测增量
			BinaryExpression blockNumBound = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
			blockNumBound.setLeft(rowNum);
//			blockNumBound.setRight(new IntIdentifier(ctx, DEFAULT_BLOCKSIZE * 2, currentFile));
			blockNumBound.setRight(new IntIdentifier(ctx, 1, currentFile));
			// blockNumCondition = (selectBlockNum <= blockNumBound)
			RelationalExpression blockNumCondition = new RelationalExpression(Expression.RelationalOp.LESSEQUAL, dwst);
			blockNumCondition.setLeft(selectBlockNum);
			blockNumCondition.setRight(blockNumBound);
			if (dVarName.equals("TH")) {
				BinaryExpression modDwhileCount = new BinaryExpression(Expression.BinaryOp.MODULUS, dwst);
				modDwhileCount.setLeft(dwhileCount);
				modDwhileCount.setRight(new IntIdentifier(3));
				RelationalExpression equalZero = new RelationalExpression(Expression.RelationalOp.EQUAL, dwst);
				equalZero.setLeft(modDwhileCount);
				equalZero.setRight(new IntIdentifier(0));

				BooleanExpression andMod = new BooleanExpression(Expression.BooleanOp.LOGICALAND, dwst);
				andMod.setLeft(blockNumCondition);
				andMod.setRight(equalZero);

				AssignmentStatement setIsDetect = new AssignmentStatement(ctx, isDetect, andMod);
				after.add(getStatementBlock(setIsDetect));

			} else {
				AssignmentStatement setIsDetect = new AssignmentStatement(ctx, isDetect, blockNumCondition);
				after.add(getStatementBlock(setIsDetect));
			}

			// if (isDetect)
			IfStatement ifDetect = new IfStatement();
			after.add(getStatementBlock(ifDetect));
			ifDetect.setConditionalPredicate(new ConditionalPredicate(isDetect));
			{
				// selectNum = sum(select)
				BuiltinFunctionExpression sumSelect = new BuiltinFunctionExpression(
						ctx, Expression.BuiltinFunctionOp.SUM, new Expression[]{select}, currentFile);
				AssignmentStatement assignSelectNum = new AssignmentStatement(ctx, selectNum, sumSelect);
				ifDetect.addStatementBlockIfBody(getStatementBlock(assignSelectNum));

				// repartitionBlockNum = [selectNum / DEFAULT_BLOCKSIZE]
				BinaryExpression approBlockNumAfterRepartition = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
				approBlockNumAfterRepartition.setLeft(selectNum);
				approBlockNumAfterRepartition.setRight(new IntIdentifier(DEFAULT_BLOCKSIZE));
				BuiltinFunctionExpression blockNumAfterRepartition = new BuiltinFunctionExpression(
						Expression.BuiltinFunctionOp.CEIL, new Expression[]{approBlockNumAfterRepartition}, dwst);
				AssignmentStatement setRepartitionBlockNum =
						new AssignmentStatement(ctx, repartitionBlockNum, blockNumAfterRepartition);
				ifDetect.addStatementBlockIfBody(getStatementBlock(setRepartitionBlockNum));

				// 若用增量, 是否 repartition
				BinaryExpression div = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
				div.setLeft(repartitionPreBlockNum);
				div.setRight(blockNumAfterRepartition);
				BinaryExpression r = new BinaryExpression(Expression.BinaryOp.POW, dwst);
				if (USE_LOCAL_SPARK_CONFIG) { // for debug
					r.setLeft(new DoubleIdentifier(1));
				} else {
					// TODO adde by czh 原4, 2.8, 2, 3
					r.setLeft(new DoubleIdentifier(2));
				}
				r.setRight(useRepartitionCount);
				RelationalExpression satisfyDiv = new RelationalExpression(Expression.RelationalOp.GREATEREQUAL, dwst);
				satisfyDiv.setLeft(div);
				satisfyDiv.setRight(r);
				RelationalExpression notZero = new RelationalExpression(Expression.RelationalOp.NOTEQUAL, dwst);
				notZero.setLeft(blockNumAfterRepartition);
				notZero.setRight(new IntIdentifier(0));
				BooleanExpression andNotZero = new BooleanExpression(Expression.BooleanOp.LOGICALAND, dwst);
				andNotZero.setLeft(satisfyDiv);
				andNotZero.setRight(notZero);
				AssignmentStatement setRepartition = new AssignmentStatement(ctx, useRepartition, andNotZero);
				ifDetect.addStatementBlockIfBody(getStatementBlock(setRepartition));

				// 下次迭代是否用增量
				// TODO added by czh 代价模型
				BooleanExpression compare = new BooleanExpression(Expression.BooleanOp.NOT, dwst);
				compare.setLeft(useDelta);
				AssignmentStatement setUseDelta = new AssignmentStatement(ctx, useDelta, compare);
				ifDetect.addStatementBlockIfBody(getStatementBlock(setUseDelta));

				// 更新 useRepartition
				BooleanExpression andUseDelta = new BooleanExpression(Expression.BooleanOp.LOGICALAND, dwst);
				andUseDelta.setLeft(useRepartition);
				andUseDelta.setRight(useDelta);
				AssignmentStatement updateRepartition = new AssignmentStatement(ctx, useRepartition, andUseDelta);
				ifDetect.addStatementBlockIfBody(getStatementBlock(updateRepartition));
			}
			// else
			{
				AssignmentStatement assignSelectNum = new AssignmentStatement(ctx, selectNum, rowNum);
				ifDetect.addStatementBlockElseBody(getStatementBlock(assignSelectNum));
			}

			// if (useDelta)
			IfStatement ifUseDelta = new IfStatement();
			after.add(getStatementBlock(ifUseDelta));
			ifUseDelta.setConditionalPredicate(new ConditionalPredicate(useDelta));
			{
				// delta = var - preVar
				BinaryExpression delta = new BinaryExpression(Expression.BinaryOp.MINUS, dwst);
				delta.setLeft(var);
				delta.setRight(preVar);
				AssignmentStatement assignDelta = new AssignmentStatement(ctx, deltaVar, delta);
				ifUseDelta.addStatementBlockIfBody(getStatementBlock(assignDelta));
			}

			// if (useRepartition)
			IfStatement ifRepartition = new IfStatement();
			after.add(getStatementBlock(ifRepartition));
			ifRepartition.setConditionalPredicate(new ConditionalPredicate(useRepartition));
			{
				// repartitionCount++
				BinaryExpression addUseRepartitionCount = new BinaryExpression(Expression.BinaryOp.PLUS, dwst);
				addUseRepartitionCount.setLeft(useRepartitionCount);
				addUseRepartitionCount.setRight(new IntIdentifier(1));
				AssignmentStatement assignRepartitionCount =
						new AssignmentStatement(ctx, useRepartitionCount, addUseRepartitionCount);
				ifRepartition.addStatementBlockIfBody(getStatementBlock(assignRepartitionCount));

				// 更新 repartitionPreBlockNum
				BinaryExpression approBlockNumAfterRepartition = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
				approBlockNumAfterRepartition.setLeft(selectNum);
				approBlockNumAfterRepartition.setRight(new IntIdentifier(DEFAULT_BLOCKSIZE));
				BuiltinFunctionExpression blockNumAfterRepartition = new BuiltinFunctionExpression(
						Expression.BuiltinFunctionOp.CEIL, new Expression[]{approBlockNumAfterRepartition}, dwst);
				AssignmentStatement assignRepartitionPreBlockNum =
						new AssignmentStatement(ctx, repartitionPreBlockNum, blockNumAfterRepartition);
				ifRepartition.addStatementBlockIfBody(getStatementBlock(assignRepartitionPreBlockNum));
			}

			// TODO added by czh debug
			BinaryExpression approBlockNumAfterRepartition = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
			approBlockNumAfterRepartition.setLeft(selectNum);
			approBlockNumAfterRepartition.setRight(new IntIdentifier(DEFAULT_BLOCKSIZE));
			BuiltinFunctionExpression blockNumAfterRepartition = new BuiltinFunctionExpression(
					Expression.BuiltinFunctionOp.CEIL, new Expression[]{approBlockNumAfterRepartition}, dwst);
			List<Expression> printArgs = new ArrayList<>(1);
			printArgs.add(blockNumAfterRepartition);
			PrintStatement print = new PrintStatement(ctx, "PRINT", printArgs, currentFile);
			after.add(getStatementBlock(print));
			List<Expression> printArgs2 = new ArrayList<>(1);
			printArgs2.add(repartitionPreBlockNum);
			PrintStatement print2 = new PrintStatement(ctx, "PRINT", printArgs2, currentFile);
			after.add(getStatementBlock(print2));

//			// 判断是否 filter
//			{
//				IfStatement ifFilter = new IfStatement();
//				after.add(getStatementBlock(ifFilter));
//
//				BinaryExpression div = new BinaryExpression(Expression.BinaryOp.DIV, dwst);
//				div.setLeft(preBlockNum);
//				div.setRight(selectBlockNum);
//				RelationalExpression compare = new RelationalExpression(Expression.RelationalOp.GREATEREQUAL, dwst);
//				compare.setLeft(div);
//				// TODO adde by czh 3
//				compare.setRight(new DoubleIdentifier(999999999));
//				BooleanExpression not = new BooleanExpression(Expression.BooleanOp.NOT, dwst);
//				not.setLeft(useRepartition);
//				BooleanExpression and = new BooleanExpression(Expression.BooleanOp.LOGICALAND, dwst);
//				and.setLeft(compare);
//				and.setRight(not);
//				ifFilter.setConditionalPredicate(new ConditionalPredicate(and));
//
//				AssignmentStatement assignPreBlockNum = new AssignmentStatement(ctx, preBlockNum, selectBlockNum);
//				AssignmentStatement enableFilter = new AssignmentStatement(ctx, useFilter, new BooleanIdentifier(true));
//				ifFilter.addStatementBlockIfBody(getStatementBlock(assignPreBlockNum));
//				ifFilter.addStatementBlockIfBody(getStatementBlock(enableFilter));
//			}
		}

		// dwhileCount++
		BinaryExpression addDwhileCount = new BinaryExpression(Expression.BinaryOp.PLUS, dwst);
		addDwhileCount.setLeft(dwhileCount);
		addDwhileCount.setRight(new IntIdentifier(1));
		AssignmentStatement updateDwhileCount = new AssignmentStatement(ctx, dwhileCount, addDwhileCount);
		after.add(getStatementBlock(updateDwhileCount));

		ConditionalPredicate predicate = new ConditionalPredicate(predicateExp);
		dwst.setPredicate(predicate);

		dwst.setDIterInit(StatementBlock.mergeStatementBlocks(init));
		dwst.setDIterBefore(StatementBlock.mergeStatementBlocks(before));
		dwst.setDIterAfter(StatementBlock.mergeStatementBlocks(after));

		// body
		if (ctx.body.size() > 0) {
			for (StatementContext stCtx : ctx.body) {
				dwst.addStatementBlock(getStatementBlock(stCtx.info.stmt));
			}

			dwst.mergeStatementBlocks();
		}

		ctx.info.stmt = dwst;
		setFileLineColumn(ctx.info.stmt, ctx);
	}

	@Override
	public void exitForStatement(ForStatementContext ctx) {
		ForStatement forStmt = new ForStatement();

		DataIdentifier iterVar = new DataIdentifier(ctx.iterVar.getText());
		HashMap<String, String> parForParamValues = null;
		Expression incrementExpr = null; //1/-1
		if (ctx.iterPred.info.increment != null) {
			incrementExpr = ctx.iterPred.info.increment;
		}
		IterablePredicate predicate = new IterablePredicate(ctx, iterVar, ctx.iterPred.info.from, ctx.iterPred.info.to,
				incrementExpr, parForParamValues, currentFile);
		forStmt.setPredicate(predicate);

		if (ctx.body.size() > 0) {
			for (StatementContext stmtCtx : ctx.body) {
				forStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			forStmt.mergeStatementBlocks();
		}
		ctx.info.stmt = forStmt;
	}

	@Override
	public void exitParForStatement(ParForStatementContext ctx) {
		ParForStatement parForStmt = new ParForStatement();

		DataIdentifier iterVar = new DataIdentifier(ctx.iterVar.getText());
		HashMap<String, String> parForParamValues = new HashMap<>();
		if (ctx.parForParams != null && ctx.parForParams.size() > 0) {
			for (StrictParameterizedExpressionContext parForParamCtx : ctx.parForParams) {
				String paramVal = parForParamCtx.paramVal.getText();
				if (argVals.containsKey(paramVal))
					paramVal = argVals.get(paramVal);
				parForParamValues.put(parForParamCtx.paramName.getText(), paramVal);
			}
		}

		Expression incrementExpr = null; //1/-1
		if (ctx.iterPred.info.increment != null) {
			incrementExpr = ctx.iterPred.info.increment;
		}
		IterablePredicate predicate = new IterablePredicate(ctx, iterVar, ctx.iterPred.info.from, ctx.iterPred.info.to,
				incrementExpr, parForParamValues, currentFile);
		parForStmt.setPredicate(predicate);
		if (ctx.body.size() > 0) {
			for (StatementContext stmtCtx : ctx.body) {
				parForStmt.addStatementBlock(getStatementBlock(stmtCtx.info.stmt));
			}
			parForStmt.mergeStatementBlocks();
		}
		ctx.info.stmt = parForStmt;
	}

	private ArrayList<DataIdentifier> getFunctionParametersNoAssign(List<TypedArgNoAssignContext> ctx) {
		ArrayList<DataIdentifier> retVal = new ArrayList<>(ctx.size());
		for (TypedArgNoAssignContext paramCtx : ctx) {
			DataIdentifier dataId = new DataIdentifier(paramCtx.paramName.getText());
			String dataType = (paramCtx.paramType == null || paramCtx.paramType.dataType() == null
					|| paramCtx.paramType.dataType().getText() == null || paramCtx.paramType.dataType().getText().isEmpty()) ?
					"scalar" : paramCtx.paramType.dataType().getText();
			String valueType = paramCtx.paramType.valueType().getText();

			//check and assign data type
			checkValidDataType(dataType, paramCtx.start);
			if (!setDataAndValueType(dataId, dataType, valueType, paramCtx.start, false, true))
				return null;
			retVal.add(dataId);
		}
		return retVal;
	}

	private ArrayList<DataIdentifier> getFunctionParametersAssign(List<TypedArgAssignContext> ctx) {
		ArrayList<DataIdentifier> retVal = new ArrayList<>(ctx.size());
		for (TypedArgAssignContext paramCtx : ctx) {
			DataIdentifier dataId = new DataIdentifier(paramCtx.paramName.getText());
			String dataType = (paramCtx.paramType == null || paramCtx.paramType.dataType() == null
					|| paramCtx.paramType.dataType().getText() == null || paramCtx.paramType.dataType().getText().isEmpty()) ?
					"scalar" : paramCtx.paramType.dataType().getText();
			String valueType = paramCtx.paramType.valueType().getText();

			//check and assign data type
			checkValidDataType(dataType, paramCtx.start);
			if (!setDataAndValueType(dataId, dataType, valueType, paramCtx.start, false, true))
				return null;
			retVal.add(dataId);
		}
		return retVal;
	}

	private ArrayList<Expression> getFunctionDefaults(List<TypedArgAssignContext> ctx) {
		return new ArrayList<>(ctx.stream().map(arg ->
				(arg.paramVal != null) ? arg.paramVal.info.expr : null).collect(Collectors.toList()));
	}

	@Override
	public void exitIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {
		ctx.info.from = ctx.from.info.expr;
		ctx.info.to = ctx.to.info.expr;
		ctx.info.increment = null;
	}

	@Override
	public void exitIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {
		if (!ctx.ID().getText().equals("seq")) {
			notifyErrorListeners("incorrect function:\'" + ctx.ID().getText() + "\'. expected \'seq\'", ctx.start);
			return;
		}
		ctx.info.from = ctx.from.info.expr;
		ctx.info.to = ctx.to.info.expr;
		if (ctx.increment != null && ctx.increment.info != null)
			ctx.info.increment = ctx.increment.info.expr;
	}


	// -----------------------------------------------------------------
	//            Internal & External Functions Definitions
	// -----------------------------------------------------------------

	@Override
	public void exitInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {
		//populate function statement
		FunctionStatement functionStmt = new FunctionStatement();
		functionStmt.setName(ctx.name.getText());
		functionStmt.setInputParams(getFunctionParametersAssign(ctx.inputParams));
		functionStmt.setInputDefaults(getFunctionDefaults(ctx.inputParams));
		functionStmt.setOutputParams(getFunctionParametersNoAssign(ctx.outputParams));

		if (ctx.body.size() > 0) {
			// handle function body
			// Create arraylist of one statement block
			ArrayList<StatementBlock> body = new ArrayList<>();
			for (StatementContext stmtCtx : ctx.body) {
				body.add(getStatementBlock(stmtCtx.info.stmt));
			}
			functionStmt.setBody(body);
			functionStmt.mergeStatementBlocks();
		} else {
			notifyErrorListeners("functions with no statements are not allowed", ctx.start);
			return;
		}

		ctx.info.stmt = functionStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
		ctx.info.functionName = ctx.name.getText();
	}

	@Override
	public void exitExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {
		//populate function statement
		ExternalFunctionStatement functionStmt = new ExternalFunctionStatement();
		functionStmt.setName(ctx.name.getText());
		functionStmt.setInputParams(getFunctionParametersNoAssign(ctx.inputParams));
		functionStmt.setOutputParams(getFunctionParametersNoAssign(ctx.outputParams));

		// set other parameters
		HashMap<String, String> otherParams = new HashMap<>();
		boolean atleastOneClassName = false;
		for (StrictParameterizedKeyValueStringContext otherParamCtx : ctx.otherParams) {
			String paramName = otherParamCtx.paramName.getText();
			String val = "";
			String text = otherParamCtx.paramVal.getText();
			// First unquote the string
			if ((text.startsWith("\"") && text.endsWith("\"")) ||
					(text.startsWith("\'") && text.endsWith("\'"))) {
				if (text.length() > 2) {
					val = text.substring(1, text.length() - 1);
				}
				// Empty value allowed
			} else {
				notifyErrorListeners("the value of user parameter for external function should be of type string",
						ctx.start);
				return;
			}
			otherParams.put(paramName, val);
			if (paramName.equals(ExternalFunctionStatement.CLASS_NAME)) {
				atleastOneClassName = true;
			}
		}
		functionStmt.setOtherParams(otherParams);
		if (!atleastOneClassName) {
			notifyErrorListeners("The \'" + ExternalFunctionStatement.CLASS_NAME
					+ "\' argument needs to be passed to the externalFunction 'implemented in' clause.", ctx.start);
			return;
		}

		ctx.info.stmt = functionStmt;
		setFileLineColumn(ctx.info.stmt, ctx);
		ctx.info.functionName = ctx.name.getText();
	}


	@Override
	public void exitPathStatement(PathStatementContext ctx) {
		PathStatement stmt = new PathStatement(ctx.pathValue.getText());
		String filePath = UtilFunctions.unquote(ctx.pathValue.getText());
		_workingDir = filePath;
		ctx.info.stmt = stmt;
	}

	@Override
	public void exitIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {
		if (!ctx.commandLineParam.getText().startsWith("$")) {
			notifyErrorListeners("the first argument of ifdef function should be a commandline argument parameter " +
					"(which starts with $)", ctx.commandLineParam.start);
			return;
		}

		if (ctx.targetList == null) {
			notifyErrorListeners("ifdef assignment needs an lvalue ", ctx.start);
			return;
		}
		String targetListText = ctx.targetList.getText();
		if (targetListText.startsWith("$")) {
			notifyErrorListeners("lhs of ifdef function cannot be a commandline parameters. Use local variable " +
					"instead", ctx.start);
			return;
		}

		DataIdentifier target = null;
		if (ctx.targetList.dataInfo.expr instanceof DataIdentifier) {
			target = (DataIdentifier) ctx.targetList.dataInfo.expr;
			Expression source = null;
			if (ctx.commandLineParam.dataInfo.expr != null) {
				// Since commandline parameter is set
				// The check of following is done in fillExpressionInfoCommandLineParameters:
				// Command line param cannot be empty string
				// If you want to pass space, please quote it
				source = ctx.commandLineParam.dataInfo.expr;
			} else {
				source = ctx.source.info.expr;
			}

			try {
				ctx.info.stmt = new AssignmentStatement(ctx, target, source, currentFile);
			} catch (LanguageException e) {
				notifyErrorListeners("invalid assignment for ifdef function", ctx.targetList.start);
				return;
			}

		} else {
			notifyErrorListeners("incorrect lvalue in ifdef function ", ctx.targetList.start);
			return;
		}
	}

	@Override
	public void exitAccumulatorAssignmentStatement(AccumulatorAssignmentStatementContext ctx) {
		if (ctx.targetList == null) {
			notifyErrorListeners("incorrect parsing for accumulator assignment", ctx.start);
			return;
		}
		//process as default assignment statement
		exitAssignmentStatementHelper(ctx, ctx.targetList.getText(),
				ctx.targetList.dataInfo, ctx.targetList.start, ctx.source.info, ctx.info);
		//mark as accumulator
		((AssignmentStatement) ctx.info.stmt).setAccumulator(true);
	}

	@Override
	public void exitMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {
		checkValidDataType(ctx.ID().getText(), ctx.start);
	}


	// -----------------------------------------------------------------
	// 			Not overridden
	// -----------------------------------------------------------------

	@Override
	public void visitTerminal(TerminalNode node) {
	}

	@Override
	public void visitErrorNode(ErrorNode node) {
	}

	@Override
	public void exitEveryRule(ParserRuleContext ctx) {
	}

	@Override
	public void enterModIntDivExpression(ModIntDivExpressionContext ctx) {
	}

	@Override
	public void enterExternalFunctionDefExpression(ExternalFunctionDefExpressionContext ctx) {
	}

	@Override
	public void enterBooleanNotExpression(BooleanNotExpressionContext ctx) {
	}

	@Override
	public void enterPowerExpression(PowerExpressionContext ctx) {
	}

	@Override
	public void enterInternalFunctionDefExpression(InternalFunctionDefExpressionContext ctx) {
	}

	@Override
	public void enterBuiltinFunctionExpression(BuiltinFunctionExpressionContext ctx) {
	}

	@Override
	public void enterConstIntIdExpression(ConstIntIdExpressionContext ctx) {
	}

	@Override
	public void enterAtomicExpression(AtomicExpressionContext ctx) {
	}

	@Override
	public void enterIfdefAssignmentStatement(IfdefAssignmentStatementContext ctx) {
	}

	@Override
	public void enterAccumulatorAssignmentStatement(AccumulatorAssignmentStatementContext ctx) {
	}

	@Override
	public void enterConstStringIdExpression(ConstStringIdExpressionContext ctx) {
	}

	@Override
	public void enterConstTrueExpression(ConstTrueExpressionContext ctx) {
	}

	@Override
	public void enterParForStatement(ParForStatementContext ctx) {
	}

	@Override
	public void enterUnaryExpression(UnaryExpressionContext ctx) {
	}

	@Override
	public void enterImportStatement(ImportStatementContext ctx) {
	}

	@Override
	public void enterPathStatement(PathStatementContext ctx) {
	}

	@Override
	public void enterWhileStatement(WhileStatementContext ctx) {
	}

	@Override
	public void enterDWhileStatement(DmlParser.DWhileStatementContext ctx) {
	}

	@Override
	public void enterCommandlineParamExpression(CommandlineParamExpressionContext ctx) {
	}

	@Override
	public void enterFunctionCallAssignmentStatement(FunctionCallAssignmentStatementContext ctx) {
	}

	@Override
	public void enterAddSubExpression(AddSubExpressionContext ctx) {
	}

	@Override
	public void enterIfStatement(IfStatementContext ctx) {
	}

	@Override
	public void enterConstDoubleIdExpression(ConstDoubleIdExpressionContext ctx) {
	}

	@Override
	public void enterMatrixMulExpression(MatrixMulExpressionContext ctx) {
	}

	@Override
	public void enterSpecialMatrixMulExpression(SpecialMatrixMulExpressionContext ctx) {
	}

	@Override
	public void enterMatrixDataTypeCheck(MatrixDataTypeCheckContext ctx) {
	}

	@Override
	public void enterCommandlinePositionExpression(CommandlinePositionExpressionContext ctx) {
	}

	@Override
	public void enterIterablePredicateColonExpression(IterablePredicateColonExpressionContext ctx) {
	}

	@Override
	public void enterAssignmentStatement(AssignmentStatementContext ctx) {
	}

	@Override
	public void enterValueType(ValueTypeContext ctx) {
	}

	@Override
	public void exitValueType(ValueTypeContext ctx) {
	}

	@Override
	public void enterMl_type(Ml_typeContext ctx) {
	}

	@Override
	public void exitMl_type(Ml_typeContext ctx) {
	}

	@Override
	public void enterBooleanAndExpression(BooleanAndExpressionContext ctx) {
	}

	@Override
	public void enterForStatement(ForStatementContext ctx) {
	}

	@Override
	public void enterRelationalExpression(RelationalExpressionContext ctx) {
	}

	@Override
	public void enterTypedArgNoAssign(TypedArgNoAssignContext ctx) {
	}

	@Override
	public void exitTypedArgNoAssign(TypedArgNoAssignContext ctx) {
	}

	@Override
	public void enterTypedArgAssign(TypedArgAssignContext ctx) {
	}

	@Override
	public void exitTypedArgAssign(TypedArgAssignContext ctx) {
	}

	@Override
	public void enterStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {
	}

	@Override
	public void exitStrictParameterizedExpression(StrictParameterizedExpressionContext ctx) {
	}

	@Override
	public void enterMultDivExpression(MultDivExpressionContext ctx) {
	}

	@Override
	public void enterConstFalseExpression(ConstFalseExpressionContext ctx) {
	}

	@Override
	public void enterStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {
	}

	@Override
	public void exitStrictParameterizedKeyValueString(StrictParameterizedKeyValueStringContext ctx) {
	}

	@Override
	public void enterProgramroot(ProgramrootContext ctx) {
	}

	@Override
	public void exitProgramroot(ProgramrootContext ctx) {
	}

	@Override
	public void enterDataIdExpression(DataIdExpressionContext ctx) {
	}

	@Override
	public void enterIndexedExpression(IndexedExpressionContext ctx) {
	}

	@Override
	public void enterParameterizedExpression(ParameterizedExpressionContext ctx) {
	}

	@Override
	public void exitParameterizedExpression(ParameterizedExpressionContext ctx) {
	}

	@Override
	public void enterFunctionCallMultiAssignmentStatement(FunctionCallMultiAssignmentStatementContext ctx) {
	}

	@Override
	public void enterIterablePredicateSeqExpression(IterablePredicateSeqExpressionContext ctx) {
	}

	@Override
	public void enterSimpleDataIdentifierExpression(SimpleDataIdentifierExpressionContext ctx) {
	}

	@Override
	public void enterBooleanOrExpression(BooleanOrExpressionContext ctx) {
	}

	@Override
	public void enterMultiIdExpression(MultiIdExpressionContext ctx) {
	}

	@Override
	public void exitMultiIdExpression(MultiIdExpressionContext ctx) {
		ArrayList<Expression> values = new ArrayList<>();
		for (ExpressionContext elem : ctx.targetList) {
			values.add(elem.info.expr);
		}
		ctx.info.expr = new ExpressionList(values);
	}

}
