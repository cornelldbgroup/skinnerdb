package expressions.compilation;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import buffer.BufferManager;
import query.ColumnRef;
import query.SQLexception;
import catalog.CatalogManager;
import catalog.info.ColumnInfo;
import config.LoggingConfig;
import data.DoubleData;
import data.IntData;
import data.LongData;
import data.StringData;
import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;
import expressions.ExpressionInfo;
import expressions.SkinnerVisitor;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;
import types.JavaType;
import types.SQLtype;
import types.TypeUtil;

import org.objectweb.asm.Type;
import org.objectweb.asm.commons.LocalVariablesSorter;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.util.Printer;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

/**
 * Writes byte code for evaluating an expression. The result of
 * each sub-expression is pushed on the stack and includes two
 * values: the actual value, if applicable, and a flag indicating
 * whether a non-null value results. The null value is on top of
 * the stack to allow quick checks for NULL values.
 * 
 * @author immanueltrummer
 *
 */
public class ExpressionCompiler extends SkinnerVisitor {
	/**
	 * Used for generating a unique class name for newly compiled
	 * expressions. Is incremented by one after each compilation.
	 */
	public static int expressionID = -1;
	/**
	 * The expression to compile and associated meta-data.
	 */
	public final ExpressionInfo expressionInfo;
	/**
	 * A mapping from columns as they appear in the expression
	 * to columns in the database (this assignment changes
	 * over different evaluation stages of the query).
	 */
	public final Map<ColumnRef, ColumnRef> columnMapping;
	/**
	 * Maps each table alias to the index at which the
	 * current tuple number can be retrieved in the
	 * tuple index vector.
	 */
	public final Map<String, Integer> tableMapping;
	/**
	 * Maps SQL aggregate expressions to columns
	 * that contain the corresponding aggregation
	 * result.
	 */
	public final Map<String, ColumnRef> aggMapping;
	/**
	 * Maps each column to an ID (used to retrieve the
	 * associated column data efficiently).
	 */
	public final Map<ColumnRef, Integer> columnToID;
	/**
	 * Maps LIKE expressions to an ID (used to retrieve
	 * pre-compiled automaton efficiently).
	 */
	public final Map<Expression, Integer> likeToID;
	/**
	 * Name of the generated class.
	 */
	public final String className;
	/**
	 * Package and name of generated class.
	 */
	public final String classAndPackage;
	/**
	 * Type of evaluator to generate.
	 */
	public final EvaluatorType evaluatorType;
	/**
	 * Writes the class of the newly defined expression evaluator.
	 */
	public final ClassWriter classWriter;
	/**
	 * Method visitor creating expression evaluation method.
	 */
	public final MethodVisitor evaluationVisitor;
	/**
	 * Used for generating local variables in evaluation method.
	 */
	public final LocalVariablesSorter evaluationLocals;
	/**
	 * Used for evaluating arithmetic expressions that
	 * involve adding a given number of months to a
	 * date or timestamp expression.
	 */
	//static Calendar calendar = Calendar.getInstance();
	/**
	 * Initializes fields and writes expression evaluator boilerplate code.
	 * 
	 * @param expressionInfo	meta-data about expression to compile
	 * @param columnMapping		maps query columns to actual columns
	 * @param tableMapping		maps table alias to tuple vector index -
	 * 							if a null vector is passed, it is assumed
	 * 							that this expression refers to one table.
	 * @param evaluatorType		type of expression evaluator
	 */
	public ExpressionCompiler(ExpressionInfo expressionInfo,
			Map<ColumnRef, ColumnRef> columnMapping,
			Map<String, Integer> tableMapping,
			Map<String, ColumnRef> aggMapping,
			EvaluatorType evaluatorType) {
		// Increment expression ID (used in class name)
		++expressionID;
		// Initialize final fields
		this.expressionInfo = expressionInfo;
		this.columnMapping = columnMapping;
		this.tableMapping = tableMapping;
		this.aggMapping = aggMapping;
		this.columnToID = new HashMap<ColumnRef, Integer>();
		this.likeToID = new HashMap<Expression, Integer>();
		this.className = "ExprEval" + expressionID;
		this.classAndPackage = "expressions/compilation/" + className;
		this.evaluatorType = evaluatorType;
		// Assign columns to IDs
		int columnID = 0;
		for (ColumnRef columnRef : expressionInfo.columnsMentioned) {
			columnToID.put(columnRef, columnID);
			++columnID;
		}
		if (aggMapping != null) {
			for (ColumnRef columnRef : aggMapping.values()) {
				columnToID.put(columnRef, columnID);
				++columnID;
			}			
		}
		// Assign regular expressions to IDs
		int regID = 0;
		for (Expression regEx : expressionInfo.likeExpressions) {
			likeToID.put(regEx, regID);
			++regID;
		}
		// Determine interface and signature
		String evalInterface = null;
		String evalSignature = null;
		switch (evaluatorType) {
		case UNARY_GENERIC:
		case KARY_GENERIC:
			break;
		case UNARY_BOOLEAN:
			evalInterface = "expressions/compilation/UnaryBoolEval";
			evalSignature = "(I)B";
			break;
		case KARY_BOOLEAN:
			evalInterface = "expressions/compilation/KnaryBoolEval";
			evalSignature = "([I)B";			
			break;
		case UNARY_INT:
			evalInterface = "expressions/compilation/UnaryIntEval";
			evalSignature = "(I[I)Z";
			break;
		case UNARY_LONG:
			evalInterface = "expressions/compilation/UnaryLongEval";
			evalSignature = "(I[J)Z";
			break;
		case UNARY_DOUBLE:
			evalInterface = "expressions/compilation/UnaryDoubleEval";
			evalSignature = "(I[D)Z";
			break;
		case UNARY_STRING:
			evalInterface = "expressions/compilation/UnaryStringEval";
			evalSignature = "(I[Ljava/lang/String;)Z";
			break;
		}
		// Log if activated
		if (LoggingConfig.COMPILATION_VERBOSE) {
			System.out.println("Column IDs:\t" + columnToID.toString());
			System.out.println("Like IDs:\t" + likeToID.toString());
			System.out.println("Class:\t" + classAndPackage);
			System.out.println("Interface:\t" + evalInterface);
			System.out.println("Signature:\t" + evalSignature);
			System.out.println("Evaluator:\t" + evaluatorType.toString());
		}
		// Create new class evaluating expression
		classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
		classWriter.visit(Opcodes.V1_8,	Opcodes.ACC_PUBLIC,
				classAndPackage, null, "java/lang/Object", 
				new String[] {evalInterface});
		// Add fields to class
		addFields(classWriter);
		// Create constructor of new class
		generateConstructor(classWriter);
		// Start writing evaluation method		
		evaluationVisitor = classWriter.visitMethod(
		        Opcodes.ACC_PUBLIC, "evaluate", 
		        evalSignature, null, null);
		evaluationVisitor.visitCode();
		// Generate local variable sorter
		evaluationLocals = new LocalVariablesSorter(
				Opcodes.ACC_PUBLIC, evalSignature, 
				evaluationVisitor);
	}
	/**
	 * Add fields to newly generated evaluator class. We add fields
	 * holding pointers to required column data and to pre-compiled
	 * automata for evaluating SQL LIKE expressions.
	 * 
	 * @param classWriter	used to write out code
	 */
	void addFields(ClassWriter classWriter) {
		// Add fields holding data of required columns
		for (Entry<ColumnRef, Integer> entry : columnToID.entrySet()) {
			ColumnRef queryRef = entry.getKey();
			// Determine name of field to create
			int columnID = entry.getValue();
			String fieldName = "D" + columnID;
			// Obtain type information from catalog
			ColumnRef dbRef = columnMapping.get(queryRef);
			ColumnInfo dbColumn = CatalogManager.getColumn(dbRef);
			JavaType javaType = TypeUtil.toJavaType(dbColumn.type);
			String typeDescriptor = null;
			switch (javaType) {
			case INT:
				typeDescriptor = "[I";
				break;
			case LONG:
				typeDescriptor = "[J";
				break;
			case DOUBLE:
				typeDescriptor = "[D";
				break;
			case STRING:
				typeDescriptor = "[Ljava/lang/String;";
				break;
			}
			classWriter.visitField(Opcodes.ACC_PUBLIC, 
					fieldName, typeDescriptor, 
					null, null);
		}
		// Add fields holding isNull flags of required columns
		for (int columnID : columnToID.values()) {
			classWriter.visitField(Opcodes.ACC_PUBLIC, 
					"N" + columnID, "Ljava/util/BitSet;", 
					null, null);
		}
		// Add fields containing compiled LIKE expressions
		for (int regID : likeToID.values()) {
			classWriter.visitField(Opcodes.ACC_PUBLIC, 
					"L" + regID, "Ldk/brics/automaton/RunAutomaton;", 
					null, null);
		}
	}
	/**
	 * Returns array holding data for an integer column.
	 * 
	 * @param tableName		name of table to retrieve
	 * @param columnName	name of column to retrieve
	 * @return				array of integer data
	 */
	public static int[] getIntData(String tableName, String columnName) {
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		IntData intData = (IntData)BufferManager.colToData.get(columnRef);
		return intData.data;
	}
	/**
	 * Returns array holding data for a long column.
	 * 
	 * @param tableName		name of the table to retrieve
	 * @param columnName	name of column to retrieve
	 * @return				array of long data
	 */
	public static long[] getLongData(String tableName, String columnName) {
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		LongData longData = (LongData)BufferManager.colToData.get(columnRef);
		return longData.data;
	}
	/**
	 * Returns array holding data for a double column.
	 * 
	 * @param tableName		name of the table to retrieve
	 * @param columnName	name of column to retrieve
	 * @return				array of double data
	 */
	public static double[] getDoubleData(String tableName, String columnName) {
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		DoubleData doubleData = (DoubleData)BufferManager.colToData.get(columnRef);
		return doubleData.data;
	}
	/**
	 * Returns array holding data for a string column. 
	 * 
	 * @param tableName		name of table to retrieve
	 * @param columnName	name of column to retrieve
	 * @return				array of string data
	 */
	public static String[] getStringData(String tableName, String columnName) {
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		StringData stringData = (StringData)BufferManager.colToData.get(columnRef);
		return stringData.data;
	}
	/**
	 * Returns BitSet representing SQL NULL values. 
	 * 
	 * @param tableName		name of table to retrieve
	 * @param columnName	name of column to retrieve
	 * @return				SQL NULL value flags
	 */	
	public static BitSet getIsNullData(String tableName, String columnName) {
		ColumnRef columnRef = new ColumnRef(tableName, columnName);
		return BufferManager.colToData.get(columnRef).isNull;
	}
	/**
	 * Extracts notNull flag from given BitSet at given position.
	 * 
	 * @param isNull	original representation of NULL flags
	 * @param row		extract flag at that index
	 * @return			integer-valued notNull flag
	 */
	public static int extractNotNull(BitSet isNull, int row) {
		return isNull.get(row)?0:1;
	}
	/**
	 * Returns an automaton that recognizes the given regular expression.
	 * 
	 * @param regEx		regular expression to compile
	 * @return			newly compiled automaton
	 */
	public static RunAutomaton compileLike(String regEx) {
        return new RunAutomaton(new RegExp(regEx).toAutomaton(), true);
	}
	/**
	 * Uses current dictionary to translate string code into string.
	 * 
	 * @param code	coded string representation
	 * @return		string associated with code value
	 */
	public static String codeToString(int code) {
		return BufferManager.dictionary.getString(code);
	}
	/**
	 * Generates code for constructor of expression evaluator. 
	 * 
	 * @param classWriter	used to write code for evaluator class
	 */	
	void generateConstructor(ClassWriter classWriter) {
		MethodVisitor constructorVisitor = classWriter.visitMethod(
		        Opcodes.ACC_PUBLIC,	// public method
		        "<init>",			// method name 
		        "()V",				// descriptor
		        null,				// signature (null means not generic)
		        null);				// exceptions (array of strings)
		constructorVisitor.visitCode();		// Start the code for this method
		constructorVisitor.visitVarInsn(Opcodes.ALOAD, 0);	// Load "this" onto the stack
		constructorVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL,	// Invoke an instance method (non-virtual)
        "java/lang/Object",			// Class on which the method is defined
        "<init>",					// Name of the method
        "()V",						// Descriptor
        false);						// Is this class an interface?
		// Fill data fields for quick access
		for (Entry<ColumnRef, Integer> entry : columnToID.entrySet()) {
			ColumnRef queryRef = entry.getKey();
			ColumnRef dbRef = columnMapping.get(queryRef);
			ColumnInfo colInfo = CatalogManager.getColumn(dbRef);
			String fieldName = "D" + entry.getValue();
			constructorVisitor.visitVarInsn(Opcodes.ALOAD, 0);
			// Assign instance field to data array
			constructorVisitor.visitLdcInsn(dbRef.aliasName);
			constructorVisitor.visitLdcInsn(dbRef.columnName);
			JavaType javaType = TypeUtil.toJavaType(colInfo.type);
			switch (javaType) {
			case INT:
				constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC,
						"expressions/compilation/ExpressionCompiler", 
						"getIntData", 
						"(Ljava/lang/String;Ljava/lang/String;)[I",
						false);
				constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
						classAndPackage, fieldName, "[I");
				break;
			case LONG:
				constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
						"expressions/compilation/ExpressionCompiler", 
						"getLongData", 
						"(Ljava/lang/String;Ljava/lang/String;)[J",
						false);
				constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
						classAndPackage, fieldName, "[J");
				break;
			case DOUBLE:
				constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
						"expressions/compilation/ExpressionCompiler", 
						"getDoubleData", 
						"(Ljava/lang/String;Ljava/lang/String;)[D",
						false);
				constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
						classAndPackage, fieldName, "[D");
				break;
			case STRING:
				constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC,
						"expressions/compilation/ExpressionCompiler", 
						"getStringData", 
						"(Ljava/lang/String;Ljava/lang/String;)"
						+ "[Ljava/lang/String;",
						false);
				constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
						classAndPackage, fieldName, "[Ljava/lang/String;");
				break;				
			}
		}
		// Fill fields holding NULL flags for quick access
		for (Entry<ColumnRef, Integer> entry : columnToID.entrySet()) {
			ColumnRef queryCol = entry.getKey();
			ColumnRef dbCol = columnMapping.get(queryCol);
			constructorVisitor.visitVarInsn(Opcodes.ALOAD, 0);
			constructorVisitor.visitLdcInsn(dbCol.aliasName);
			constructorVisitor.visitLdcInsn(dbCol.columnName);
			constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
					"expressions/compilation/ExpressionCompiler",
					"getIsNullData", "(Ljava/lang/String;Ljava/lang/String;)"
							+ "Ljava/util/BitSet;", false);
			int columnID = entry.getValue();
			String fieldName = "N" + columnID;
			constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
					classAndPackage, fieldName, "Ljava/util/BitSet;");
		}
		// Fields holding compiled LIKE expressions
		for (Entry<Expression, Integer> entry : likeToID.entrySet()) {
			// Translate SQL like expression into Java regex
			String regex = ((StringValue)(entry.getKey())).getValue();
			// Replace special symbols
			for (char c : new char[] {'.', '(', ')', '[', ']', '{', '}'}) {
				regex = regex.replace(c + "", "\\" + c);
			}
	        regex = regex.replace('?', '.');
	        regex = regex.replace("%", ".*");
	        // Create corresponding automaton and store it
	        constructorVisitor.visitVarInsn(Opcodes.ALOAD, 0);
	        constructorVisitor.visitLdcInsn(regex);
	        constructorVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
	        		"expressions/compilation/ExpressionCompiler", 
	        		"compileLike", 
	        		"(Ljava/lang/String;)Ldk/brics/automaton/RunAutomaton;", 
	        		false);
	        String fieldName = "L" + entry.getValue();
	        constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD, 
	        		classAndPackage, fieldName, 
	        		"Ldk/brics/automaton/RunAutomaton;");
		}
		constructorVisitor.visitInsn(Opcodes.RETURN);
		constructorVisitor.visitMaxs(1, 1);
		constructorVisitor.visitEnd();
	}
	/**
	 * Add code for printing out given string.
	 * 
	 * @param output	output to print
	 */
	void addPrintString(String output) {
		if (LoggingConfig.EVALUATION_ROW_VERBOSE) {
			evaluationVisitor.visitFieldInsn(Opcodes.GETSTATIC, 
					"java/lang/System", "out", "Ljava/io/PrintStream;");
			evaluationVisitor.visitLdcInsn(output);
			evaluationVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, 
					"java/io/PrintStream", "println", 
					"(Ljava/lang/String;)V", false);			
		}
	}
	/**
	 * Add code for storing stack content to interface fields.
	 * 
	 * @param javaType		java result type
	 * @param keepContent	whether to restore stack content after use
	 */
	void storeStackContent(JavaType javaType, boolean keepContent) 
			throws Exception {
		// Add code for storing evaluation result null flag
		if (keepContent) {
			evaluationVisitor.visitInsn(Opcodes.DUP);
		}
		evaluationVisitor.visitInsn(Opcodes.ICONST_1);
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitInsn(Opcodes.ISUB);
		evaluationVisitor.visitFieldInsn(Opcodes.PUTSTATIC, 
				"expressions/compilation/ExpressionInterface", 
				"nullResult", "B");
		// Add code for storing evaluation result
		// TODO: need to distinguish double/long from int/ref here 
		if (keepContent) {
			evaluationVisitor.visitInsn(Opcodes.SWAP);
			evaluationVisitor.visitInsn(Opcodes.DUP);
		}
		switch (javaType) {
		case INT:
			evaluationVisitor.visitFieldInsn(Opcodes.PUTSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"intResult", "I");
			break;
		case LONG:
			evaluationVisitor.visitFieldInsn(Opcodes.PUTSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"longResult", "J");
			break;
		case DOUBLE:
			evaluationVisitor.visitFieldInsn(Opcodes.PUTSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"doubleResult", "D");
			break;
		case STRING:
			evaluationVisitor.visitFieldInsn(Opcodes.PUTSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"stringResult", "Ljava/lang/String;");
			break;
		default:
			throw new Exception("Unsupported expression result type: " + 
					javaType.toString());
		}
		if (keepContent) {
			evaluationVisitor.visitInsn(Opcodes.SWAP);
		}
	}
	/**
	 * Add code for printing out stack content.
	 * 
	 * @param javaType	java type
	 */
	void addPrintStack(JavaType javaType) {
		if (LoggingConfig.EVALUATION_ROW_VERBOSE) {
			try {
				storeStackContent(javaType, true);
				evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
						"expressions/compilation/ExpressionInterface", 
						"print", "()V", false);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * Returns freshly generated expression evaluator.
	 * 
	 * @return	instance of newly generated evaluator class
	 * @throws Exception
	 */
	/*
	public ExpressionEvaluator getCompiledEvaluator() throws Exception {
		// Copy stack content to expression interface
		JavaType resultType = TypeUtil.toJavaType(
				expressionInfo.resultType);
		storeStackContent(resultType, false);
		// Finalize evaluation method
		evaluationVisitor.visitInsn(Opcodes.RETURN);
		evaluationVisitor.visitMaxs(-1, -1);
		evaluationVisitor.visitEnd();
		// Finalize class definition
		classWriter.visitEnd();
		// Print out generated bytecode
		if (LoggingConfig.COMPILATION_VERBOSE) {
			outputBytecode(classWriter);			
		}
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className, 
				classWriter.toByteArray());
		return (ExpressionEvaluator)expressionClass.newInstance();
	}
	*/
	/**
	 * Add code to swap two values of given Java type.
	 * The values may either correspond to one single
	 * or to two words on the stack.
	 * 
	 * @param type1		type of first (lower) element to swap
	 * @param type2		type of second (upper) element to swap
	 */
	void smartSwap(JavaType type1, JavaType type2) {
		// Determine whether we swap multi-word values
		boolean multi1 = type1.equals(JavaType.LONG) ||
				type1.equals(JavaType.DOUBLE);
		boolean multi2 = type2.equals(JavaType.LONG) ||
				type2.equals(JavaType.DOUBLE);
		// Add corresponding swap command
		if (!multi1 && !multi2) {
			// Swap single-word values
			evaluationVisitor.visitInsn(Opcodes.SWAP);
		} else if (multi1 && multi2) {
			// Swap two multi-word values
			evaluationVisitor.visitInsn(Opcodes.DUP2_X2);
			evaluationVisitor.visitInsn(Opcodes.POP2);
		} else if (!multi1 && multi2) {
			// Swap lower single word against upper multi-word value
			evaluationVisitor.visitInsn(Opcodes.DUP2_X1);
			evaluationVisitor.visitInsn(Opcodes.POP2);
		} else {
			// Swap lower multi-word against upper single word value
			evaluationVisitor.visitInsn(Opcodes.DUP_X2);
			evaluationVisitor.visitInsn(Opcodes.POP);
		}
	}
	/**
	 * Pops a single or multi-word value from the stack.
	 * Decide based on input type which operation to use.
	 * 
	 * @param jType	pop one value of this type
	 */
	void smartPop(JavaType jType) {
		// Is this a multi-word value?
		boolean multi = jType.equals(JavaType.LONG) ||
				jType.equals(JavaType.DOUBLE);
		// Apply appropriate pop operation
		if (multi) {
			evaluationVisitor.visitInsn(Opcodes.POP2);
		} else {
			evaluationVisitor.visitInsn(Opcodes.POP);
		}
	}
	/**
	 * Instantiates a new class for evaluating Boolean expressions.
	 * Can be either unary or k-nary Boolean expressions (a cast
	 * is required after invocation).
	 * 
	 * @return	newly generated evaluator instance
	 * @throws Exception
	 */
	public Object getBoolEval() throws Exception {
		if (!sqlExceptions.isEmpty()) {
			throw sqlExceptions.get(0);
		}
		// (Booleans are represented as integers -
		// hence we can use standard swaps).
		// Turn 0/1 value into -1/+1 value
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitInsn(Opcodes.ICONST_2);
		evaluationVisitor.visitInsn(Opcodes.IMUL);
		evaluationVisitor.visitInsn(Opcodes.ICONST_M1);
		evaluationVisitor.visitInsn(Opcodes.IADD);
		// Multiply not null flag with scaled value
		evaluationVisitor.visitInsn(Opcodes.IMUL);
		// Finalize evaluation method
		evaluationVisitor.visitInsn(Opcodes.IRETURN);
		evaluationVisitor.visitMaxs(-1, -1);
		evaluationVisitor.visitEnd();
		// Finalize class definition
		classWriter.visitEnd();
		// Print out generated bytecode
		if (LoggingConfig.COMPILATION_VERBOSE) {
			outputBytecode(classWriter);			
		}
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className,
				classWriter.toByteArray());
		return expressionClass.newInstance();
	}
	/**
	 * Finalizes code for unary expression evaluator
	 * (except for Boolean evaluator which is treated
	 * separately).
	 * 
	 * @throws Exception
	 */
	void finalizeUnaryEval() throws Exception {
		if (!sqlExceptions.isEmpty()) {
			throw sqlExceptions.get(0);
		}
		// Obtain result type
		SQLtype type = expressionInfo.resultType;
		JavaType jType = TypeUtil.toJavaType(type);
		// (result, null flag) -> (null flag, result)
		smartSwap(jType, JavaType.INT);
		// Get reference to array for storing result
		evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 2);
		// First array reference then value to store
		// (result, array) -> (array, result)
		smartSwap(jType, JavaType.INT);
		// Put array index on stack
		evaluationVisitor.visitInsn(Opcodes.ICONST_0);
		// First array reference, then index, then value to store
		// (result, integer) -> (integer, result)
		smartSwap(jType, JavaType.INT);
		// Store result value in array
		switch (jType) {
		case INT:
			evaluationVisitor.visitInsn(Opcodes.IASTORE);
			break;
		case LONG:
			evaluationVisitor.visitInsn(Opcodes.LASTORE);
			break;
		case DOUBLE:
			evaluationVisitor.visitInsn(Opcodes.DASTORE);
			break;
		case STRING:
			evaluationVisitor.visitInsn(Opcodes.AASTORE);
			break;
		}
		// Remaining value is not null flag - return it
		evaluationVisitor.visitInsn(Opcodes.IRETURN);
		// Print out generated bytecode
		if (LoggingConfig.COMPILATION_VERBOSE) {
			outputBytecode(classWriter);			
		}
		evaluationVisitor.visitMaxs(-1, -1);
		evaluationVisitor.visitEnd();
		// Finalize class definition
		classWriter.visitEnd();
	}
	/**
	 * Instantiates a new class for evaluating unary
	 * integer expressions.
	 * 
	 * @return	newly generated evaluator object
	 * @throws Exception
	 */
	public UnaryIntEval getUnaryIntEval() throws Exception {
		// Finalize code for unary evaluator of integer result
		finalizeUnaryEval();
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className,
				classWriter.toByteArray());
		return (UnaryIntEval)expressionClass.newInstance();
	}
	/**
	 * Instantiates a new class for evaluating unary
	 * long expressions.
	 * 
	 * @return	newly generated evaluator object
	 * @throws Exception
	 */
	public UnaryLongEval getUnaryLongEval() throws Exception {
		// Finalize code for unary evaluator of integer result
		finalizeUnaryEval();
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className,
				classWriter.toByteArray());
		return (UnaryLongEval)expressionClass.newInstance();
	}
	/**
	 * Instantiates a new class for evaluating unary
	 * double expressions.
	 * 
	 * @return	newly generated evaluator object
	 * @throws Exception
	 */
	public UnaryDoubleEval getUnaryDoubleEval() throws Exception {
		// Finalize code for unary evaluator of integer result
		finalizeUnaryEval();
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className,
				classWriter.toByteArray());
		return (UnaryDoubleEval)expressionClass.newInstance();
	}
	/**
	 * Instantiates new class for evaluating unary
	 * string expression.
	 * 
	 * @return	newly generated evaluator object
	 * @throws Exception
	 */
	public UnaryStringEval getUnaryStringEval() throws Exception {
		// Finalize code for unary evaluator of string result
		finalizeUnaryEval();
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> expressionClass = loader.defineClass(
				"expressions.compilation." + className,
				classWriter.toByteArray());
		return (UnaryStringEval)expressionClass.newInstance();
	}
	/**
	 * Output byte code of generated class.
	 * 
	 * @param classWriter	writer for class
	 */
	public static void outputBytecode(ClassWriter classWriter) {
	    ClassReader classReader = new ClassReader(classWriter.toByteArray());
	    ClassNode classNode = new ClassNode();
	    classReader.accept(classNode, 0);
	    List<MethodNode> methodNodes = classNode.methods;
	    Printer printer = new Textifier();
	    int lineCtr = 1;
	    TraceMethodVisitor traceVisitor = new TraceMethodVisitor(printer);
	    for (FieldNode fieldNode : (List<FieldNode>) classNode.fields) {
	    	System.out.println(lineCtr + "\t" + fieldNode.name);
	    	++lineCtr;
	    }
	    for (MethodNode methodNode : methodNodes) {
	        InsnList instructions = methodNode.instructions;
	        System.out.println(methodNode.name);
	        lineCtr++;
	        for (int i = 0; i < instructions.size(); i++) {
	            instructions.get(i).accept(traceVisitor);
	            StringWriter stringWriter = new StringWriter();
	            printer.print(new PrintWriter(stringWriter));
	            printer.getText().clear();
	            System.out.print(lineCtr);
	            System.out.print(stringWriter.toString());
	            ++lineCtr;
	        }
	    }
	}
	/**
	 * Return java type for given expression.
	 * 
	 * @param expression	retrieve type of this expression
	 * @return	java type of input expression
	 */
	JavaType jType(Expression expression) {
		SQLtype type = expressionInfo.expressionToType.get(expression);
		return TypeUtil.toJavaType(type);
	}

	@Override
	public void visit(NullValue arg0) {
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
	}

	@Override
	public void visit(Function arg0) {
		String fct = arg0.getName().toLowerCase();
		switch (fct) {
		case "min":
		case "max":
		case "avg":
		case "sum":
		case "count":
			// We have an aggregation function -
			// we expect that such expressions have been
			// evaluated before compilation is invoked.
			String SQL = arg0.toString();
			ColumnRef aggRef = aggMapping.get(SQL);
			String alias = aggRef.aliasName;
			Table table = new Table(alias);
			String column = aggRef.columnName;
			Column aggCol = new Column(table, column);
			visit(aggCol);
			break;
		default:
			/*
			// We assume a user-defined predicate by default -
			// evaluate input parameters.
			for (Expression param : arg0.getParameters().getExpressions()) {
				param.accept(this);
			}
			// Generate signature of method invocation
			StringBuilder signatureBuilder = new StringBuilder();
			for (Expression param : arg0.getParameters().getExpressions()) {
				
			}
			// Invoke function (assumed to be static!)
			evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC,
					"expressions/compilation/ExpressionCompiler", 
					"getIntData", 
					"(Ljava/lang/String;Ljava/lang/String;)[I",
					false);

			break;
			*/
		}
	}

	@Override
	public void visit(SignedExpression arg0) {
		arg0.getExpression().accept(this);
		if (arg0.getSign() == '-') {
			JavaType jType = jType(arg0);
			// Put value on top of stack
			// (value, null flag) -> (null flag, value)
			smartSwap(jType, JavaType.INT);
			// Apply sign
			switch (jType) {
			case INT:
				evaluationVisitor.visitInsn(Opcodes.ICONST_M1);
				evaluationVisitor.visitInsn(Opcodes.IMUL);
				break;
			case LONG:
				evaluationVisitor.visitLdcInsn((long)1);
				evaluationVisitor.visitInsn(Opcodes.LMUL);
				break;
			case DOUBLE:
				evaluationVisitor.visitLdcInsn((double)1.0);
				evaluationVisitor.visitInsn(Opcodes.DMUL);
				break;
			default:
				System.out.println("Signed expression unsupported");
				break;
			}
			// Put known flag back on top
			// (null flag, value) -> (value, null flag)
			smartSwap(JavaType.INT, jType);
		}
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcNamedParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		evaluationVisitor.visitLdcInsn(arg0.getValue());
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(LongValue arg0) {
		// Either treat as integer or as long
		SQLtype type = expressionInfo.expressionToType.get(arg0);
		long value = arg0.getValue();
		switch (type) {
		case INT:
			evaluationVisitor.visitLdcInsn((int)value);
			break;
		case LONG:
			evaluationVisitor.visitLdcInsn(value);
			break;
		default:
			System.out.println("Warning: unsupported type for long");
		}
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(HexValue arg0) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void visit(DateValue arg0) {
		// Transform date into int value
		int unixTime = (int)(arg0.getValue().getTime()/1000);
		evaluationVisitor.visitLdcInsn(unixTime);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(TimeValue arg0) {
		// Transform time into long value
		int unixTime = (int)(arg0.getValue().getTime()/1000);
		evaluationVisitor.visitLdcInsn(unixTime);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(TimestampValue arg0) {
		// Transform timestamp into long value
		int unixTime = (int)(arg0.getValue().getTime()/1000);
		evaluationVisitor.visitLdcInsn(unixTime);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		// Initialize string code
		int code = -1;
		// Try to find code in dictionary
		if (CatalogManager.currentDB.compressed) {
			code = BufferManager.dictionary.getCode(arg0.getValue());
		}
		// Did we find value in dictionary?
		if (code >= 0) {
			evaluationVisitor.visitLdcInsn(code);
		} else {
			evaluationVisitor.visitLdcInsn(arg0.getValue());
		}
		// Set not null flag
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		// Generate debugging output
		addPrintString(arg0.toString());
		addPrintStack((code >= 0?JavaType.INT:JavaType.STRING));
	}
	/**
	 * Returns true iff the left operand is either a date or
	 * a timestamp and the right operand is a year-month
	 * time interval.
	 * 
	 * @param left		left operand of binary expression
	 * @param right		right operand of binary expression
	 * @return			true iff the operands require special treatment
	 */
	boolean dateYMintervalOp(Expression left, Expression right) {
		SQLtype leftType = expressionInfo.expressionToType.get(left);
		SQLtype rightType = expressionInfo.expressionToType.get(right);
		return (leftType.equals(SQLtype.DATE) || 
				leftType.equals(SQLtype.TIMESTAMP)) &&
				(rightType.equals(SQLtype.YM_INTERVAL));
	}
	/**
	 * Adds given number of months to a date, represented
	 * according to Unix time format.
	 * 
	 * @param dateSecs	seconds since January 1st 1970
	 * @param months	number of months to add (can be negative)
	 * @return			seconds since January 1st 1970 after addition
	 */
	public static int addMonths(int dateSecs, int months) {
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTimeInMillis((long)dateSecs * (long)1000);
			calendar.add(Calendar.MONTH, months);			
		} catch (Exception e) {
			System.out.println("dateSecs: " + dateSecs);
			System.out.println("months: " + months);
			System.out.println(calendar);
			e.printStackTrace();
		}
		return (int)(calendar.getTimeInMillis()/(long)1000);
	}
	/**
	 * Adds code to treat the addition or subtraction of a
	 * number of months from a date or timestamp value.
	 * Returns true iff the given expression is indeed
	 * of that type.
	 * 
	 * @param arg0	expression potentially involving months arithmetic
	 * @return		true if code for expression was added
	 */
	boolean treatAsMonthArithmetic(BinaryExpression arg0) {
		Expression left = arg0.getLeftExpression();
		Expression right = arg0.getRightExpression();
		if (dateYMintervalOp(left, right) ||
				dateYMintervalOp(right, left)) {
			// Swap input parameters if required
			if (dateYMintervalOp(right, left)) {
				evaluationVisitor.visitInsn(Opcodes.SWAP);
			}
			// Multiply number of months by -1 for subtraction
			if (arg0 instanceof Subtraction) {
				evaluationVisitor.visitLdcInsn(-1);
				evaluationVisitor.visitInsn(Opcodes.IMUL);
			}
			// Invoke auxiliary function for adding months
			evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC,
					"expressions/compilation/ExpressionCompiler", 
					"addMonths", "(II)I", false);
			return true;
		} else {
			return false;
		}
	}
	/**
	 * Adds byte code for a binary arithmetic operation.
	 * Also invokes this visitor recursively.
	 * 
	 * @param binaryExpression	expression to treat
	 * @param intOp				operation in case of integer inputs
	 * @param longOp			operation in case of long inputs
	 * @param floatOp			operation in case of float inputs
	 * @param doubleOp			operation in case of double inputs
	 * @param readableName		operation name to display in case of errors
	 */
	void treatBinaryArithmetic(BinaryExpression binaryExpression,
			int intOp, int longOp, int floatOp, int doubleOp, 
			String readableName) {
		Label firstNull = new Label();
		Label secondNull = new Label();
		Label theEnd = new Label();
		binaryExpression.getLeftExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, firstNull);
		binaryExpression.getRightExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, secondNull);
		// Treat case that both input operands are not null
		JavaType jType = jType(binaryExpression.getLeftExpression());
		if (!treatAsMonthArithmetic(binaryExpression)) {
			switch (jType) {
			case INT:
				evaluationVisitor.visitInsn(intOp);
				break;
			case LONG:
				evaluationVisitor.visitInsn(longOp);
				break;
			case DOUBLE:
				evaluationVisitor.visitInsn(doubleOp);
				break;
			default:
				System.err.println("Warning: unsupported types in " + 
						readableName);
			}
		}
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		evaluationVisitor.visitLabel(secondNull);
		smartPop(jType);
		evaluationVisitor.visitLabel(firstNull);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitLabel(theEnd);
	}
	/**
	 * Adds code implementing a binary comparison - after executing
	 * that code, the stack contains 1 if the condition is satisfied
	 * and 0 otherwise. Also invokes this visitor recursively.
	 * 
	 * @param binaryExpression	binary expression to evaluate
	 * @param intOp				jump instruction for integer operands
	 * @param longOp			jump instruction for long operands
	 * @param floatOp			jump instruction for float operands
	 * @param doubleOp			jump instruction for double operands
	 * @param readableName		comparison name for warning messages
	 */
	void treatBinaryCmp(BinaryExpression binaryExpression, 
			int intOp, int nonIntOp, String readableName) {
		// Define labels
		Label ifTrue = new Label();
		Label endIf = new Label();
		Label firstNull = new Label();
		Label secondNull = new Label();
		binaryExpression.getLeftExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, firstNull);
		binaryExpression.getRightExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, secondNull);
		// Distinguish operand types and prepare jump to true label
		SQLtype type = expressionInfo.expressionToType.get(
				binaryExpression.getLeftExpression());
		JavaType jType = TypeUtil.toJavaType(type);
		switch (jType) {
		case INT:
			evaluationVisitor.visitJumpInsn(intOp, ifTrue);
			break;
		case LONG:
			// 0 if equal, 1 if first greater, -1 if first smaller
			evaluationVisitor.visitInsn(Opcodes.LCMP);
			evaluationVisitor.visitJumpInsn(nonIntOp, ifTrue);
			break;
		case DOUBLE:
			// 0 if equal, 1 if first greater, -1 if first smaller
			evaluationVisitor.visitInsn(Opcodes.DCMPG);
			evaluationVisitor.visitJumpInsn(nonIntOp, ifTrue);
			break;
		case STRING:
			// 0 if equal, >0 if first greater, <0 if first smaller
 			evaluationVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, 
					"java/lang/String", "compareTo", 
					"(Ljava/lang/String;)I", false);
 			evaluationVisitor.visitJumpInsn(nonIntOp, ifTrue);
 			break;
		}
		// This code is only invoked if condition not satisfied
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, endIf);
		// This code is only invoked if condition is satisfied
		evaluationVisitor.visitLabel(ifTrue);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, endIf);
		evaluationVisitor.visitLabel(secondNull);
		smartPop(jType);
		evaluationVisitor.visitLabel(firstNull);
		smartPop(jType);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		// The following label marks the end of the if statement
		evaluationVisitor.visitLabel(endIf);	
		// Print stack content in debugging mode
		addPrintString("Stack after treating:" + 
				binaryExpression.toString());
		addPrintStack(jType);
	}
	
	@Override
	public void visit(Addition arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IADD, Opcodes.LADD, 
				Opcodes.FADD, Opcodes.DADD, "addition");
	}

	@Override
	public void visit(Division arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IDIV, Opcodes.LDIV, 
				Opcodes.FDIV, Opcodes.DDIV, "division");
	}

	@Override
	public void visit(Multiplication arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IMUL, Opcodes.LMUL,
				Opcodes.FMUL, Opcodes.DMUL, "multiplication");
	}

	@Override
	public void visit(Subtraction arg0) {
		treatBinaryArithmetic(arg0, Opcodes.ISUB, Opcodes.LSUB,
				Opcodes.FSUB, Opcodes.DSUB, "subtraction");
	}

	@Override
	public void visit(AndExpression arg0) {
		// (Can use standard swaps and pops
		// as we are dealing with integer
		// values only.)
		Label firstNull = new Label();
		Label firstFalse = new Label();
		Label evaluate2nd = new Label();
		Label secondNull = new Label();
		Label secondFalse = new Label();		
		Label theEnd = new Label();
		// Evaluate left expression and process results
		arg0.getLeftExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, firstNull);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, firstFalse);
		// If first one is true
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, evaluate2nd);
		// If first one is null
		evaluationVisitor.visitLabel(firstNull);
		evaluationVisitor.visitInsn(Opcodes.POP);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, evaluate2nd);
		// If first one is false
		evaluationVisitor.visitLabel(firstFalse);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// Evaluate right expression
		evaluationVisitor.visitLabel(evaluate2nd);
		arg0.getRightExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, secondNull);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, secondFalse);
		// If second is true - top stack element indicates whether
		// first one was true (=1) or null (=0)
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// If second is null
		evaluationVisitor.visitLabel(secondNull);
		evaluationVisitor.visitInsn(Opcodes.POP2);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// If second is false
		evaluationVisitor.visitLabel(secondFalse);
		evaluationVisitor.visitInsn(Opcodes.POP);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		//evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		evaluationVisitor.visitLabel(theEnd);
	}

	@Override
	public void visit(OrExpression arg0) {
		// (Can use standard swap as we are
		// dealing with integer values only).
		Label firstNull = new Label();
		Label firstTrue = new Label();
		Label evaluate2nd = new Label();
		Label secondNull = new Label();
		Label secondTrue = new Label();
		Label theEnd = new Label();
		arg0.getLeftExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, firstNull);
		evaluationVisitor.visitJumpInsn(Opcodes.IFNE, firstTrue);
		// If first one evaluates to false
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, evaluate2nd);
		// If first one is null
		evaluationVisitor.visitLabel(firstNull);
		evaluationVisitor.visitInsn(Opcodes.POP);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, evaluate2nd);
		// If first one evaluates to true
		evaluationVisitor.visitLabel(firstTrue);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// Evaluate right expression
		evaluationVisitor.visitLabel(evaluate2nd);
		arg0.getRightExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, secondNull);
		evaluationVisitor.visitJumpInsn(Opcodes.IFNE, secondTrue);
		// If second is false - top stack element indicates whether
		// first one was known to be false (=1) or null (=0)
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// If second is null
		evaluationVisitor.visitLabel(secondNull);
		evaluationVisitor.visitInsn(Opcodes.POP2);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// If second is true
		evaluationVisitor.visitLabel(secondTrue);
		evaluationVisitor.visitInsn(Opcodes.POP);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		//evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		evaluationVisitor.visitLabel(theEnd);
	}

	@Override
	public void visit(Between arg0) {
		System.out.println("Error: between should have been removed");
	}

	@Override
	public void visit(EqualsTo arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPEQ, 
				Opcodes.IFEQ, "equal");
	}

	@Override
	public void visit(GreaterThan arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPGT,
				Opcodes.IFGT, "greater than");
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPGE,
				Opcodes.IFGE, "greater or equals");
	}

	@Override
	public void visit(InExpression arg0) {
		System.err.println("Error: IN should have been replaced");
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// Process input expression
		Expression input = arg0.getLeftExpression();
		input.accept(this);
		// Get input type
		JavaType jType = jType(input);
		// Value below, not null flag on top
		// (value, null flag) -> (null flag, value)
		smartSwap(jType, JavaType.INT);
		// Value on top, not null flag below
		smartPop(jType);
		// Discarded value, kept only not null flag
		if (arg0.isNot()) {
			// test if not null, i.e. if not null flag is 1
			// -> nothing to be done here
		} else {
			// test if null, i.e. if not null flag is 0
			evaluationVisitor.visitInsn(Opcodes.ICONST_1);
			evaluationVisitor.visitInsn(Opcodes.SWAP);
			evaluationVisitor.visitInsn(Opcodes.ISUB);
		}
		// Set not null flag
		evaluationVisitor.visitInsn(Opcodes.ICONST_1);
	}

	@Override
	public void visit(LikeExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Label isNull = new Label();
		Label theEnd = new Label();
		// Skip evaluation if argument is null
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, isNull);
		// Execute expression matching
		evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 0);
		int likeID = likeToID.get(arg0.getRightExpression());
		String fieldName = "L" + likeID;
		evaluationVisitor.visitFieldInsn(Opcodes.GETFIELD, 
				classAndPackage, fieldName, 
				"Ldk/brics/automaton/RunAutomaton;");
		// (value, this) -> (this, value)
		// (value is string or string code)
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		// Stack: automaton, expression
		evaluationVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, 
				"dk/brics/automaton/RunAutomaton", "run", 
				"(Ljava/lang/String;)Z", false);
		// Treat negation
		if (arg0.isNot()) {
			evaluationVisitor.visitInsn(Opcodes.ICONST_1);
			// (swap between two integers)
			evaluationVisitor.visitInsn(Opcodes.SWAP);
			evaluationVisitor.visitInsn(Opcodes.ISUB);
		}
		// Put not null flag on top and end
		evaluationVisitor.visitInsn(Opcodes.ICONST_1);
		evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		// Execute in case of null value
		evaluationVisitor.visitLabel(isNull);
		// (pops string reference or string code)
		evaluationVisitor.visitInsn(Opcodes.POP);
		evaluationVisitor.visitInsn(Opcodes.ICONST_0);	// don't care
		evaluationVisitor.visitInsn(Opcodes.ICONST_0);	// null value
		// Evaluation end
		evaluationVisitor.visitLabel(theEnd);
	}

	@Override
	public void visit(MinorThan arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPLT, 
				Opcodes.IFLT, "less than");
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPLE,
				Opcodes.IFLE, "less or equal");
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		treatBinaryCmp(arg0, Opcodes.IF_ICMPNE,
				Opcodes.IFNE, "not equal");
	}

	@Override
	public void visit(Column arg0) {
		//System.out.println(arg0);
		// Get name of field holding column data
		String alias = arg0.getTable().getName();
		String columnName = arg0.getColumnName();
		ColumnRef queryRef = new ColumnRef(alias, columnName);
		int columnID = columnToID.get(queryRef);
		String fieldName = "D" + columnID;
		// Get description of field type
		ColumnRef dbRef = columnMapping.get(queryRef);
		ColumnInfo colInfo = CatalogManager.getColumn(dbRef);
		JavaType javaType = TypeUtil.toJavaType(colInfo.type);
		String fieldDescriptor = null;
		switch (javaType) {
		case INT:
			fieldDescriptor = "[I";
			break;
		case LONG:
			fieldDescriptor = "[J";
			break;
		case DOUBLE:
			fieldDescriptor = "[D";
			break;
		case STRING:
			fieldDescriptor = "[Ljava/lang/String;";
			break;
		}
		// Retrieve data array
		evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 0);
		evaluationVisitor.visitFieldInsn(Opcodes.GETFIELD, 
				classAndPackage, fieldName, fieldDescriptor);
		// Retrieve tuple index
		switch (evaluatorType) {
		case UNARY_GENERIC:
		{
			evaluationVisitor.visitFieldInsn(Opcodes.GETSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"unaryIndex", "I");
		}
		break;
		case KARY_GENERIC:
		{
			int tableIndex = tableMapping.get(alias);
			evaluationVisitor.visitFieldInsn(Opcodes.GETSTATIC, 
					"expressions/compilation/ExpressionInterface", 
					"tupleIndices", "[I");
			evaluationVisitor.visitLdcInsn(tableIndex);
			evaluationVisitor.visitInsn(Opcodes.IALOAD);
		}
		break;
		case UNARY_BOOLEAN:
		case UNARY_INT:
		case UNARY_LONG:
		case UNARY_DOUBLE:
		case UNARY_STRING:
		{
			evaluationVisitor.visitVarInsn(Opcodes.ILOAD, 1);
		}
		break;
		case KARY_BOOLEAN:
		{
			evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 1);
			int tableIndex = tableMapping.get(alias);
			evaluationVisitor.visitLdcInsn(tableIndex);
			evaluationVisitor.visitInsn(Opcodes.IALOAD);
		}
		break;
		}
		// Generate local variable for storing index
		int tupleIdxVar = evaluationLocals.newLocal(Type.INT_TYPE);
		evaluationVisitor.visitVarInsn(Opcodes.ISTORE, tupleIdxVar);
		// Access array at given position
		evaluationVisitor.visitVarInsn(Opcodes.ILOAD, tupleIdxVar);
		switch (javaType) {
		case INT:
			evaluationVisitor.visitInsn(Opcodes.IALOAD);
			break;
		case LONG:
			evaluationVisitor.visitInsn(Opcodes.LALOAD);
			break;
		case DOUBLE:
			evaluationVisitor.visitInsn(Opcodes.DALOAD);
			break;
		case STRING:
			evaluationVisitor.visitInsn(Opcodes.AALOAD);
			break;
		}
		// Add code for putting not-null flag on stack
		evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 0);
		evaluationVisitor.visitFieldInsn(Opcodes.GETFIELD, 
				classAndPackage, "N" + columnID, "Ljava/util/BitSet;");
		evaluationVisitor.visitVarInsn(Opcodes.ILOAD, tupleIdxVar);
		evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
				"expressions/compilation/ExpressionCompiler", 
				"extractNotNull", "(Ljava/util/BitSet;I)I", false);
		// Print stack content in debugging mode
		addPrintString("Stack after treating:" + arg0.toString());
		addPrintStack(javaType);
	}
	
	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		List<Expression> whenExprs = arg0.getWhenClauses();
		int nrWhenExprs = whenExprs.size();
		// Generate labels
		Label theEnd = new Label();
		List<Label> whenLabels = new ArrayList<>();
		for (int whenCtr=0; whenCtr<nrWhenExprs; ++whenCtr) {
			whenLabels.add(new Label());
		}
		// Iterate over when expressions
		for (int whenCtr=0; whenCtr<nrWhenExprs; ++whenCtr) {
			// Evaluate when condition
			Expression whenExpr = whenExprs.get(whenCtr);
			WhenClause whenClause = (WhenClause)whenExpr;
			whenClause.getWhenExpression().accept(this);
			// Combine null flag and evaluation result
			evaluationVisitor.visitInsn(Opcodes.IMUL);
			// Jump if condition is satisfied
			Label whenLabel = whenLabels.get(whenCtr);
			evaluationVisitor.visitJumpInsn(
					Opcodes.IFNE, whenLabel);
		}
		// Do we have an else expression?
		Expression elseExpr = arg0.getElseExpression();
		if (elseExpr != null) {
			elseExpr.accept(this);
			evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		} else {
			// No else expression specified - return null
			SQLtype resultType = expressionInfo.expressionToType.get(arg0);
			JavaType jResultType = TypeUtil.toJavaType(resultType);
			switch (jResultType) {
			case INT:
				evaluationVisitor.visitLdcInsn((int)0);
				break;
			case LONG:
				evaluationVisitor.visitLdcInsn((long)0);
				break;
			case DOUBLE:
				evaluationVisitor.visitLdcInsn((double)0);
				break;
			case STRING:
				evaluationVisitor.visitLdcInsn("");
				break;
			}
			evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
			evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		}
		// Evaluate when expressions
		for (int whenCtr=0; whenCtr<nrWhenExprs; ++whenCtr) {
			// Add associated jump target
			Label whenLabel = whenLabels.get(whenCtr);
			evaluationVisitor.visitLabel(whenLabel);
			// Evaluate when condition
			Expression whenExpr = whenExprs.get(whenCtr);
			WhenClause whenClause = (WhenClause)whenExpr;
			whenClause.getThenExpression().accept(this);
			evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
		}
		evaluationVisitor.visitLabel(theEnd);
	}

	@Override
	public void visit(WhenClause arg0) {
		// WHEN clauses are handled when visiting
		// the surrounding CASE expression!
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		Label isNull = new Label();
		Label theEnd = new Label();
		arg0.getLeftExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, isNull);
		arg0.getRightExpression().accept(this);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, isNull);
        evaluationVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
        		"java.lang.String", "concat",
        		"(Ljava/lang/String;)Ljava/lang/String;", false);
        evaluationVisitor.visitJumpInsn(Opcodes.GOTO, theEnd);
        evaluationVisitor.visitLabel(isNull);
        evaluationVisitor.visitLdcInsn("");
        evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 0);
        evaluationVisitor.visitLabel(theEnd);
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IAND, 
				Opcodes.LAND, -1, -1, "BitwiseAnd");
	}

	@Override
	public void visit(BitwiseOr arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IOR, 
				Opcodes.LOR, -1, -1, "BitwiseOr");
	}

	@Override
	public void visit(BitwiseXor arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IXOR,
				Opcodes.LXOR, -1, -1, "BitwiseXor");
	}

	@Override
	public void visit(CastExpression arg0) {
		// Get source and target type
		SQLtype sourceType = expressionInfo.expressionToType.get(
				arg0.getLeftExpression());
		SQLtype targetType = expressionInfo.expressionToType.get(arg0);
		// Put result of left expression on stack
		arg0.getLeftExpression().accept(this);
		// (value, null) -> (null, value)
		JavaType jSourceType = TypeUtil.toJavaType(sourceType);
		smartSwap(jSourceType, JavaType.INT);
		// Add command for casting
		switch (sourceType) {
		case INT:
			switch (targetType) {
			case LONG:
				evaluationVisitor.visitInsn(Opcodes.I2L);
				break;
			case DOUBLE:
				evaluationVisitor.visitInsn(Opcodes.I2D);
				break;
			}
			break;
		case LONG:
			switch (targetType) {
			case INT:
				evaluationVisitor.visitInsn(Opcodes.L2I);
				break;
			case DOUBLE:
				evaluationVisitor.visitInsn(Opcodes.L2D);
				break;
			}
			break;
		case DOUBLE:
			switch (targetType) {
			case INT:
				evaluationVisitor.visitInsn(Opcodes.D2I);
				break;
			case LONG:
				evaluationVisitor.visitInsn(Opcodes.D2L);
				break;
			}
			break;
		case STRING_CODE:
			switch (targetType) {
			case STRING:
				evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
						"expressions/compilation/ExpressionCompiler", 
						"codeToString", "(I)Ljava/lang/String;", false);
				break;
			}
			break;
		}
		// (null, target value) -> (target value, null)
		JavaType jTargetType = TypeUtil.toJavaType(targetType);
		smartSwap(JavaType.INT, jTargetType);
	}

	@Override
	public void visit(Modulo arg0) {
		treatBinaryArithmetic(arg0, Opcodes.IREM, Opcodes.LREM, 
				Opcodes.FREM, Opcodes.DREM, "modulo");
	}

	@Override
	public void visit(AnalyticExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithinGroupExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	/**
	 * Extract specific part from date given as Unix time.
	 * 
	 * @param dateSecs		seconds since 1st of January 1970
	 * @param partID		ID of extracted part
	 * @return				extracted time unit (integer)
	 */
	public static int extractFromDate(int dateSecs, int partID) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis((long)dateSecs * (long)1000);
		return calendar.get(partID);
	}

	@Override
	public void visit(ExtractExpression arg0) {
		// Verify that input is of type 'date'
		// Translate extracted part name into ID
		String part = arg0.getName().toLowerCase();
		Map<String, Integer> partToID = new HashMap<>();
		// microseconds not supported
		partToID.put("second", Calendar.SECOND);
		partToID.put("minute", Calendar.MINUTE);
		partToID.put("hour", Calendar.HOUR);
		partToID.put("day", Calendar.DAY_OF_MONTH);
		partToID.put("month", Calendar.MONTH);
		// quarter currently not supported
		partToID.put("year", Calendar.YEAR);
		// second_microsecond not supported
		// minute_microsecond not supported
		// ...
		if (!partToID.containsKey(part)) {
			sqlExceptions.add(new SQLexception("Error - "
					+ "unsupported extraction part: " 
					+ part));
		}
		int partID = partToID.get(part);
		// Add code for evaluating input expression
		arg0.getExpression().accept(this);
		// Put date seconds on top
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitLdcInsn(partID);
		evaluationVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, 
				"expressions/compilation/ExpressionCompiler", 
				"extractFromDate", "(II)I", false);
		// Put not-null flag back on top
		evaluationVisitor.visitInsn(Opcodes.SWAP);
	}

	@Override
	public void visit(IntervalExpression arg0) {
		// Extract parameter value
		String param = arg0.getParameter();
		String strVal = param.substring(1, param.length()-1);
		int intVal = Integer.valueOf(strVal);
		// Treat according to interval type
		String intervalType = arg0.getIntervalType().toLowerCase();
		switch (intervalType) {
		case "year":
			evaluationVisitor.visitLdcInsn(intVal * 12);
			break;
		case "month":
			evaluationVisitor.visitLdcInsn(intVal);
			break;
		case "day":
			int daySecs = 24 * 60 * 60 * intVal;
			evaluationVisitor.visitLdcInsn(daySecs);
			break;
		case "hour":
			int hourSecs = 60 * 60 * intVal;
			evaluationVisitor.visitLdcInsn(hourSecs);
			break;
		case "minute":
			int minuteSecs = 60 * intVal;
			evaluationVisitor.visitLdcInsn(minuteSecs);
			break;
		case "second":
			evaluationVisitor.visitLdcInsn(intVal);
			break;
		default:
			System.out.println("Error - unknown interval type");
		}
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
	}

	@Override
	public void visit(OracleHierarchicalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(KeepExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MySQLGroupConcat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RowConstructor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHint arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeKeyExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateTimeLiteralExpression arg0) {
		switch (arg0.getType()) {
		case DATE:
			DateValue dateVal = new DateValue(arg0.getValue());
			visit(dateVal);
			break;
		case TIME:
			TimeValue timeVal = new TimeValue(arg0.getValue());
			visit(timeVal);
			break;
		case TIMESTAMP:
			TimestampValue tsVal = new TimestampValue(arg0.getValue());
			visit(tsVal);
			break;
		}
	}

	@Override
	public void visit(NotExpression arg0) {
		// (Can use standard swaps since
		// input values are Boolean).
		arg0.getExpression().accept(this);
		// Bring Boolean value to top of stack
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		// Calculate new Boolean value
		evaluationVisitor.visitInsn(Opcodes.ICONST_1);
		evaluationVisitor.visitInsn(Opcodes.SWAP);
		evaluationVisitor.visitInsn(Opcodes.ISUB);
		// Bring not-null flag on top of stack again
		evaluationVisitor.visitInsn(Opcodes.SWAP);
	}
}
