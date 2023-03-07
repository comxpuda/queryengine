package com.comxpuda.physicalplan

import com.comxpuda.datatypes.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.VarCharVector

class SelectionExec(val input: PhysicalPlan, val expr: Expression) : PhysicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun execute(): Sequence<RecordBatch> {
        val input = input.execute()
        return input.map { batch ->
            val result = ((expr.evaluate(batch)) as ArrowFieldVector).field as BitVector
            val schema = batch.schema
            val columnCount = schema.fields.size
            val filteredFields = (0 until columnCount).map { filter(batch.field(it), result) }
            val fields = filteredFields.map { ArrowFieldVector(it) }
            RecordBatch(schema, fields)
        }
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    private fun filter(v: ColumnVector, selection: BitVector): FieldVector {
        val filteredVector = VarCharVector("v", RootAllocator(Long.MAX_VALUE))
        filteredVector.allocateNew()

        val builder = ArrowVectorBuilder(filteredVector)

        var count = 0
        (0 until selection.valueCount).forEach {
            if (selection.get(it) == 1) {
                builder.set(count, v.getValue(it))
                count++
            }
        }
        filteredVector.valueCount = count
        return filteredVector
    }
}
