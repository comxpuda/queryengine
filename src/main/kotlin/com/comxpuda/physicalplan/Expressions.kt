package com.comxpuda.physicalplan

import com.comxpuda.datatypes.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.types.pojo.ArrowType
import kotlin.math.ln
import kotlin.math.sqrt

interface Expression {

    /**
     * Evaluate the expression against an input record batch and produce a column of data as output
     */
    fun evaluate(input: RecordBatch): ColumnVector

}

class LiteralLongExpression(val value: Long) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(ArrowTypes.Int64Type, value, input.rowCount())
    }

}

class LiteralStringExpression(val value: String) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(ArrowTypes.StringType, value.toByteArray(), input.rowCount())
    }
}

class ColumnExpression(val i: Int) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }

    override fun toString(): String {
        return "#$i"
    }

}

/** Base class for unary math expressions */
abstract class UnaryMathExpression(private val expr: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val n = expr.evaluate(input);
        val v = Float8Vector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until n.size()).forEach {
            val nv = n.getValue(it)
            if (nv == null) {
                v.setNull(it)
            } else if (nv is Double) {
                v.set(it, sqrt(nv))
            } else {
                TODO()
            }
        }
        return ArrowFieldVector(v)
    }

    abstract fun apply(value: Double): Double
}

/** Square root */
class Sqrt(expr: Expression) : UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return sqrt(value)
    }
}

/** Natural logarithm */
class Log(expr: Expression) : UnaryMathExpression(expr) {
    override fun apply(value: Double): Double {
        return ln(value)
    }
}


abstract class BinaryExpression(val l: Expression, val r: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        assert(ll.size() == rr.size())
        if (ll.getType() != rr.getType()) {
            throw IllegalStateException(
                "Binary expression operands do not have the same type: " +
                        "${ll.getType()} != ${rr.getType()}"
            )
        }
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector
}

abstract class BooleanExpression(val l: Expression, val r: Expression) : Expression {

    override fun evaluate(input: RecordBatch): ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        assert(ll.size() == rr.size())
        if (ll.getType() != rr.getType()) {
            throw IllegalStateException(
                "Cannot compare values of different type: " +
                        "${ll.getType()} != ${rr.getType()}"
            )
        }
        return compare(ll, rr)
    }

    fun compare(l: ColumnVector, r: ColumnVector): ColumnVector {
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until l.size()).forEach {
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            v.set(it, if (value) 1 else 0)
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean
}

class AndExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) && toBool(r)
    }
}

class OrExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) || toBool(r)
    }
}

private fun toBool(v: Any?): Boolean {
    return when (v) {
        is Boolean -> v
        is Number -> v.toInt() == 1
        else -> throw java.lang.IllegalStateException()
    }
}

class EqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) == (r as Short)
            ArrowTypes.Int32Type -> (l as Int) == (r as Int)
            ArrowTypes.Int64Type -> (l as Long) == (r as Long)
            ArrowTypes.FloatType -> (l as Float) == (r as Float)
            ArrowTypes.DoubleType -> (l as Double) == (r as Double)
            ArrowTypes.StringType -> toString(l) == toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class NeqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) != (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) != (r as Short)
            ArrowTypes.Int32Type -> (l as Int) != (r as Int)
            ArrowTypes.Int64Type -> (l as Long) != (r as Long)
            ArrowTypes.FloatType -> (l as Float) != (r as Float)
            ArrowTypes.DoubleType -> (l as Double) != (r as Double)
            ArrowTypes.StringType -> toString(l) != toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) < (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) < (r as Short)
            ArrowTypes.Int32Type -> (l as Int) < (r as Int)
            ArrowTypes.Int64Type -> (l as Long) < (r as Long)
            ArrowTypes.FloatType -> (l as Float) < (r as Float)
            ArrowTypes.DoubleType -> (l as Double) < (r as Double)
            ArrowTypes.StringType -> toString(l) < toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtEqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) <= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) <= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) <= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) <= (r as Long)
            ArrowTypes.FloatType -> (l as Float) <= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) <= (r as Double)
            ArrowTypes.StringType -> toString(l) <= toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) > (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) > (r as Short)
            ArrowTypes.Int32Type -> (l as Int) > (r as Int)
            ArrowTypes.Int64Type -> (l as Long) > (r as Long)
            ArrowTypes.FloatType -> (l as Float) > (r as Float)
            ArrowTypes.DoubleType -> (l as Double) > (r as Double)
            ArrowTypes.StringType -> toString(l) > toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtEqExpression(l: Expression, r: Expression) : BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) >= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) >= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) >= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) >= (r as Long)
            ArrowTypes.FloatType -> (l as Float) >= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) >= (r as Double)
            ArrowTypes.StringType -> toString(l) >= toString(r)
            else ->
                throw java.lang.IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

private fun toString(v: Any?): String {
    return when (v) {
        is ByteArray -> String(v)
        else -> v.toString()
    }
}

abstract class MathExpression(l: Expression, r: Expression) : BinaryExpression(l, r) {
    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
        val fieldVector = FieldVectorFactory.create(l.getType(), l.size())
        val builder = ArrowVectorBuilder(fieldVector)
        (0 until l.size()).forEach {
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            builder.set(it, value)
        }
        builder.setValueCount(l.size())
        return builder.build()
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any?
}

class Add(l: Expression, r: Expression) : MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) + (r as Short)
            ArrowTypes.Int32Type -> (l as Int) + (r as Int)
            ArrowTypes.Int64Type -> (l as Long) + (r as Long)
            ArrowTypes.FloatType -> (l as Float) + (r as Float)
            ArrowTypes.DoubleType -> (l as Double) + (r as Double)
            else -> throw java.lang.IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l+$r"
    }

}

class SubtractExpression(l: Expression, r: Expression) : MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) - (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) - (r as Short)
            ArrowTypes.Int32Type -> (l as Int) - (r as Int)
            ArrowTypes.Int64Type -> (l as Long) - (r as Long)
            ArrowTypes.FloatType -> (l as Float) - (r as Float)
            ArrowTypes.DoubleType -> (l as Double) - (r as Double)
            else -> throw java.lang.IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l-$r"
    }
}

class MultiplyExpression(l: Expression, r: Expression) : MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) * (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) * (r as Short)
            ArrowTypes.Int32Type -> (l as Int) * (r as Int)
            ArrowTypes.Int64Type -> (l as Long) * (r as Long)
            ArrowTypes.FloatType -> (l as Float) * (r as Float)
            ArrowTypes.DoubleType -> (l as Double) * (r as Double)
            else -> throw java.lang.IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l*$r"
    }
}

class DivideExpression(l: Expression, r: Expression) : MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) / (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) / (r as Short)
            ArrowTypes.Int32Type -> (l as Int) / (r as Int)
            ArrowTypes.Int64Type -> (l as Long) / (r as Long)
            ArrowTypes.FloatType -> (l as Float) / (r as Float)
            ArrowTypes.DoubleType -> (l as Double) / (r as Double)
            else -> throw java.lang.IllegalStateException("Unsupported data type in math expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l/$r"
    }
}

class CastExpression(val expr: Expression, val dataType: ArrowType) : Expression {

    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }

    override fun evaluate(input: RecordBatch): ColumnVector {
        val value: ColumnVector = expr.evaluate(input)
        val fieldVector = FieldVectorFactory.create(dataType, input.rowCount())
        val builder = ArrowVectorBuilder(fieldVector)

        when (dataType) {
            ArrowTypes.Int8Type -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toByte()
                                is String -> vv.toByte()
                                is Number -> vv.toByte()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Byte: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.Int16Type -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toShort()
                                is String -> vv.toShort()
                                is Number -> vv.toShort()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Short: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.Int32Type -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toInt()
                                is String -> vv.toInt()
                                is Number -> vv.toInt()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Int: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.Int64Type -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toLong()
                                is String -> vv.toLong()
                                is Number -> vv.toLong()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Long: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.FloatType -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toFloat()
                                is String -> vv.toFloat()
                                is Number -> vv.toFloat()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Float: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.DoubleType -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toDouble()
                                is String -> vv.toDouble()
                                is Number -> vv.toDouble()
                                else -> throw java.lang.IllegalStateException("Cannot cast value to Double: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }

            ArrowTypes.StringType -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        builder.set(it, vv.toString())
                    }
                }
            }

            else -> throw java.lang.IllegalStateException("Cast to $dataType is not supported")
        }

        builder.setValueCount(value.size())
        return builder.build()
    }
}


interface AggregateExpression {
    fun inputExpression(): Expression
    fun createAccumulator(): Accumulator
}

interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}

class MaxExpression(private val expr: Expression) : AggregateExpression {
    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MaxAccumulator()
    }

    override fun toString(): String {
        return "MAX($expr)"
    }

}

class MinExpression(private val expr: Expression) : AggregateExpression {
    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return MinAccumulator()
    }

    override fun toString(): String {
        return "MAX($expr)"
    }

}

class SumExpression(private val expr: Expression) : AggregateExpression {
    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return SumAccumulator()
    }

    override fun toString(): String {
        return "MAX($expr)"
    }

}

class MaxAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMax = when (value) {
                    is Byte -> value > this.value as Byte
                    is Short -> value > this.value as Short
                    is Int -> value > this.value as Int
                    is Long -> value > this.value as Long
                    is Float -> value > this.value as Float
                    is Double -> value > this.value as Double
                    is String -> value > this.value as String
                    else -> throw UnsupportedOperationException(
                        "MAX is not implemented for data type: ${value.javaClass.name}"
                    )
                }

                if (isMax) {
                    this.value = value
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }

}

class MinAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val isMin = when (value) {
                    is Byte -> value < this.value as Byte
                    is Short -> value < this.value as Short
                    is Int -> value < this.value as Int
                    is Long -> value < this.value as Long
                    is Float -> value < this.value as Float
                    is Double -> value < this.value as Double
                    is String -> value < this.value as String
                    else -> throw UnsupportedOperationException(
                        "MAX is not implemented for data type: ${value.javaClass.name}"
                    )
                }

                if (isMin) {
                    this.value = value
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }

}

class SumAccumulator : Accumulator {

    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                val currentValue = this.value
                when (currentValue) {
                    is Byte -> this.value = currentValue + value as Byte
                    is Short -> this.value = currentValue + value as Short
                    is Int -> this.value = currentValue + value as Int
                    is Long -> this.value = currentValue + value as Long
                    is Float -> this.value = currentValue + value as Float
                    is Double -> this.value = currentValue + value as Double
                    else ->
                        throw java.lang.UnsupportedOperationException(
                            "MIN is not implemented for type: ${value.javaClass.name}"
                        )
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }

}