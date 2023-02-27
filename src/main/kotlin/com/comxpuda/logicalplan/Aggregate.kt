package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Schema

class Aggregate(val input: LogicalPlan, val groupExpr: List<LogicalExpr>, val aggregateExpr: List<AggregateExpr>) :
    LogicalPlan {
    override fun schema(): Schema {
        return Schema(groupExpr.map { it.toFiled(input) } + aggregateExpr.map { it.toFiled(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
    }
}