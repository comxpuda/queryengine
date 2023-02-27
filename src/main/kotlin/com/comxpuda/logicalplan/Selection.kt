package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Schema

class Selection(val input: LogicalPlan, val expr: LogicalExpr) : LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}