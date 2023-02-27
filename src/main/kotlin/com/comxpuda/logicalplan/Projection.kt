package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Schema

class Projection(val input: LogicalPlan, val expr: List<LogicalExpr>) : LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toFiled(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Projection: ${expr.map { it.toString() }.joinToString(", ")}"
    }
}