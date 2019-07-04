package expressions.compilation;


import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import types.JavaType;
import types.TypeUtil;

public class TestCompiler {
	public static void main(String[] args) throws Exception {
		int[] testArray = new int[] {3, 9, 128};
		// Create new class evaluating expression
		ClassWriter classWriter = new ClassWriter(
				ClassWriter.COMPUTE_MAXS |
				ClassWriter.COMPUTE_FRAMES);
		classWriter.visit(Opcodes.V1_8,		// Java 1.8 
        Opcodes.ACC_PUBLIC,					// public class
        "expressions/compilation/testclass",	// package and name
        null,								// signature (null means not generic)
        "java/lang/Object",					// superclass
        new String[]{ "expressions/compilation/ExpressionEvaluator" }); // interfaces
		// Add test fields
		classWriter.visitField(Opcodes.ACC_PUBLIC, 
				"stringtest", Type.getDescriptor(String.class), 
				null, null);
		// Create constructor of new class
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
		constructorVisitor.visitVarInsn(Opcodes.ALOAD, 0);
		constructorVisitor.visitLdcInsn("thisisatest");
		constructorVisitor.visitFieldInsn(Opcodes.PUTFIELD,
				"expressions/compilation/testclass", 
				"stringtest", "Ljava/lang/String;");
		constructorVisitor.visitInsn(Opcodes.RETURN);	// End the constructor method
		constructorVisitor.visitMaxs(1, 1);				// Specify max stack and local vars
		constructorVisitor.visitEnd();
		// Start writing evaluation method		
		MethodVisitor evaluationVisitor = classWriter.visitMethod(
		        Opcodes.ACC_PUBLIC,	// public method
		        "evaluate",			// name
		        "()V",				// descriptor
		        null,				// signature (null means not generic)
		        null);				// exceptions (array of strings)
		evaluationVisitor.visitCode();
		// Access instance field
		evaluationVisitor.visitVarInsn(Opcodes.ALOAD, 0);
		evaluationVisitor.visitFieldInsn(Opcodes.GETFIELD, 
				"expressions/compilation/testclass", 
				"stringtest", "Ljava/lang/String;");
		Label testLabel = new Label();
		evaluationVisitor.visitLdcInsn("test");
		evaluationVisitor.visitLabel(testLabel);
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, testLabel);
		evaluationVisitor.visitLdcInsn("test2");
		evaluationVisitor.visitIntInsn(Opcodes.BIPUSH, 1);
		evaluationVisitor.visitJumpInsn(Opcodes.IFEQ, testLabel);
		// Finalize evaluation method
		evaluationVisitor.visitInsn(Opcodes.RETURN);
		// Specify max stack and local variables
		evaluationVisitor.visitMaxs(-1, -1);
		evaluationVisitor.visitEnd();
		// Finalize class definition
		classWriter.visitEnd();
		ExpressionCompiler.outputBytecode(classWriter);
		// Create instance of freshly generated class
		DynamicClassLoader loader = new DynamicClassLoader();
		Class<?> unaryPredClass = loader.defineClass(
				"expressions.compilation.testclass", 
				classWriter.toByteArray());
		ExpressionInterface.print();
		((ExpressionEvaluator)unaryPredClass.newInstance()).evaluate();
		ExpressionInterface.print();
	}
	/*
	 * mv = cw.visitMethod(ACC_PUBLIC + ACC_STATIC, "printOne", "()V", null, null);
mv.visitCode();

mv.visitFieldInsn(GETSTATIC, "java/lang/System", "err", "Ljava/io/PrintStream;");
mv.visitLdcInsn("CALL println");
mv.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);

mv.visitFieldInsn(GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
mv.visitLdcInsn("Hello World");
mv.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);

mv.visitFieldInsn(GETSTATIC, "java/lang/System", "err", "Ljava/io/PrintStream;");
mv.visitLdcInsn("RETURN println");
mv.visitMethodInsn(INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V", false);

mv.visitInsn(RETURN);
mv.visitMaxs(2, 0);
mv.visitEnd();
	 */
}

/*
1	D0
2	N0
<init>
4    ALOAD 0
5    INVOKESPECIAL java/lang/Object.<init> ()V
6    ALOAD 0
7    LDC "info_type"
8    LDC "info"
9    INVOKESTATIC expressions/compilation/ExpressionCompiler.getStringData (Ljava/lang/String;Ljava/lang/String;)[Ljava/lang/String;
10    PUTFIELD expressions/compilation/ExprEval0.D0 : [Ljava/lang/String;
11    ALOAD 0
12    LDC "info_type"
13    LDC "info"
14    INVOKESTATIC expressions/compilation/ExpressionCompiler.getIsNullData (Ljava/lang/String;Ljava/lang/String;)Ljava/util/BitSet;
15    PUTFIELD expressions/compilation/ExprEval0.N0 : Ljava/util/BitSet;
16    RETURN
evaluate
18    LDC "test"
19    BIPUSH 1
20    IFEQ L0
21    LDC "test"
22    BIPUSH 1
23    IFEQ L1
24    INVOKEVIRTUAL java/lang/String.compareTo (Ljava/lang/String;)I
25    IFEQ L2
26    BIPUSH 0
27    BIPUSH 1
28    GOTO L3
29   L2
30   FRAME SAME
31    BIPUSH 1
32    BIPUSH 1
33    GOTO L3
34   L1
35   FRAME FULL [expressions/compilation/ExprEval0] [java/lang/String java/lang/String]
36    POP
37   L0
38   FRAME SAME1 java/lang/String
39    BIPUSH 0
40   L3
41   FRAME FULL [expressions/compilation/ExprEval0] [T I]
42    ICONST_1
43    SWAP
44    ISUB
45    PUTSTATIC expressions/compilation/ExpressionInterface.nullResult : B
46    PUTSTATIC expressions/compilation/ExpressionInterface.boolResult : B
47    RETURN
*/