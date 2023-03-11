package com.comxpuda.optimizer

import com.comxpuda.logicalplan.*

class Optimizer() {

    fun optimize(plan: LogicalPlan): LogicalPlan {
        // note there is only one rule implemented so far but later there will be a list
        val rule = ProjectionPushDownRule()
        return rule.optimize(plan)
    }
}

interface OptimizerRule {
    fun optimize(plan: LogicalPlan): LogicalPlan
}

fun extractColumns(expr: List<LogicalExpr>, input: LogicalPlan, accum: MutableSet<String>) {
    expr.forEach { extractColumns(it, input, accum) }
}

fun extractColumns(expr: LogicalExpr, input: LogicalPlan, accum: MutableSet<String>) {
    when (expr) {
        is ColumnIndex -> accum.add(input.schema().fields[expr.index].name)
        is Column -> accum.add(expr.name)
        is BinaryExpr -> {
            extractColumns(expr.l, input, accum)
            extractColumns(expr.r, input, accum)
        }

        is Alias -> extractColumns(expr.expr, input, accum)
        is CastExpr -> extractColumns(expr.expr, input, accum)
        is LiteralString -> {}
        is LiteralLong -> {}
        is LiteralDouble -> {}
        else -> throw IllegalStateException("extractColumns does not support expression: $expr")
    }
}
