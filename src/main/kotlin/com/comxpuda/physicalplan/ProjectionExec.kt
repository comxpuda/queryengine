package com.comxpuda.physicalplan

import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema

class ProjectionExec(val input: PhysicalPlan, val schema: Schema, val expr: List<Expression>) : PhysicalPlan {
    override fun schema(): Schema {
        return schema
    }

    override fun execute(): Sequence<RecordBatch> {
        return input.execute().map { batch ->
            val columnVectors = expr.map { it.evaluate(batch) }
            RecordBatch(schema, columnVectors)
        }
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "ProjectionExec: $expr"
    }
}