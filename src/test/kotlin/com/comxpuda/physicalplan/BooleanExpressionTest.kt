package com.comxpuda.physicalplan

import com.comxpuda.datatypes.ArrowTypes
import com.comxpuda.datatypes.Field
import com.comxpuda.datatypes.Schema
import com.comxpuda.fuzzer.Fuzzer
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BooleanExpressionTest {

    @Test
    fun `gteq bytes`() {

        val schema = Schema(listOf(Field("a", ArrowTypes.Int8Type), Field("b", ArrowTypes.Int8Type)))

        val a: List<Byte> = listOf(10, 20, 30, Byte.MIN_VALUE, Byte.MAX_VALUE)
        val b: List<Byte> = listOf(10, 30, 20, Byte.MAX_VALUE, Byte.MIN_VALUE)

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    @Test
    fun `gteq shorts`() {

        val schema = Schema(listOf(Field("a", ArrowTypes.Int16Type), Field("b", ArrowTypes.Int16Type)))

        val a: List<Short> = listOf(111, 222, 333, Short.MIN_VALUE, Short.MAX_VALUE)
        val b: List<Short> = listOf(111, 333, 222, Short.MAX_VALUE, Short.MIN_VALUE)

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    @Test
    fun `gteq ints`() {

        val schema = Schema(listOf(Field("a", ArrowTypes.Int32Type), Field("b", ArrowTypes.Int32Type)))

        val a: List<Int> = listOf(111, 222, 333, Int.MIN_VALUE, Int.MAX_VALUE)
        val b: List<Int> = listOf(111, 333, 222, Int.MAX_VALUE, Int.MIN_VALUE)

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    @Test
    fun `gteq longs`() {

        val schema = Schema(listOf(Field("a", ArrowTypes.Int64Type), Field("b", ArrowTypes.Int64Type)))

        val a: List<Long> = listOf(111, 222, 333, Long.MIN_VALUE, Long.MAX_VALUE)
        val b: List<Long> = listOf(111, 333, 222, Long.MAX_VALUE, Long.MIN_VALUE)

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    @Test
    fun `gteq doubles`() {

        val schema =
            Schema(listOf(Field("a", ArrowTypes.DoubleType), Field("b", ArrowTypes.DoubleType)))

        val a: List<Double> = listOf(0.0, 1.0, Double.MIN_VALUE, Double.MAX_VALUE, Double.NaN)
        val b = a.reversed()

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    @Test
    fun `gteq strings`() {

        val schema =
            Schema(listOf(Field("a", ArrowTypes.StringType), Field("b", ArrowTypes.StringType)))

        val a: List<String> = listOf("aaa", "bbb", "ccc")
        val b: List<String> = listOf("aaa", "ccc", "bbb")

        val batch = Fuzzer().createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

}