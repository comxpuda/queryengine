package com.comxpuda.queryplanner

import com.comxpuda.datatypes.Schema
import com.comxpuda.logicalplan.*
import com.comxpuda.physicalplan.*
import java.sql.SQLException

/** The query planner creates a physical query plan from a logical query plan. */
class QueryPlanner {

    fun createPhysicalPlan(plan: LogicalPlan): PhysicalPlan {
        val physicalPlan = when (plan) {
            is Scan -> {
                ScanExec(plan.dataSource, plan.projection)
            }

            is Selection -> {
                val input = createPhysicalPlan(plan.input)
                val filterExpr = createPhysicalExpr(plan.expr, plan.input)
                SelectionExec(input, filterExpr)
            }

            is Projection -> {
                val input = createPhysicalPlan(plan.input)
                val projectionExpr = plan.expr.map { createPhysicalExpr(it, plan.input) }
                val projectionSchema = Schema(plan.expr.map { it.toFiled(plan.input) })
                ProjectionExec(input, projectionSchema, projectionExpr)
            }

            is Aggregate -> {
                val input = createPhysicalPlan(plan.input)
                val groupExpr = plan.groupExpr.map { createPhysicalExpr(it, plan.input) }
                val aggregateExpr = plan.aggregateExpr.map {
                    when (it) {
                        is Max -> MaxExpression(createPhysicalExpr(it.expr, plan.input))
                        is Min -> MinExpression(createPhysicalExpr(it.expr, plan.input))
                        is Sum -> SumExpression(createPhysicalExpr(it.expr, plan.input))
                        else -> throw java.lang.IllegalStateException("Unsupported aggregate function: $it")
                    }
                }
                HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
            }

            else -> throw IllegalStateException(plan.javaClass.toString())
        }
        return physicalPlan
    }

    fun createPhysicalExpr(expr: LogicalExpr, input: LogicalPlan): Expression =
        when (expr) {
            is LiteralLong -> LiteralLongExpression(expr.n)
            is LiteralDouble -> LiteralDoubleExpression(expr.n)
            is LiteralString -> LiteralStringExpression(expr.str)
            is ColumnIndex -> ColumnExpression(expr.index)
            is Alias -> {
                // note that there is no physical expression for an alias since the alias
                // only affects the name using in the planning phase and not how the aliased
                // expression is executed
                createPhysicalExpr(expr.expr, input)
            }

            is Column -> {
                val i = input.schema().fields.indexOfFirst { it.name == expr.name }
                if (i == -1) {
                    throw SQLException("No column named '${expr.name}'")
                }
                ColumnExpression(i)
            }

            is CastExpr -> CastExpression(createPhysicalExpr(expr.expr, input), expr.dataType)
            is BinaryExpr -> {
                val l = createPhysicalExpr(expr.l, input)
                val r = createPhysicalExpr(expr.r, input)
                when (expr) {
                    // comparision
                    is Eq -> EqExpression(l, r)
                    is Neq -> NeqExpression(l, r)
                    is Gt -> GtExpression(l, r)
                    is GtEq -> GtEqExpression(l, r)
                    is Lt -> LtExpression(l, r)
                    is LtEq -> LtEqExpression(l, r)

                    // boolean
                    is And -> AndExpression(l, r)
                    is Or -> OrExpression(l, r)

                    // math
                    is Add -> AddExpression(l, r)
                    is Subtract -> SubtractExpression(l, r)
                    is Multiply -> MultiplyExpression(l, r)
                    is Divide -> DivideExpression(l, r)
                    else -> throw IllegalStateException("Unsupported binary expression: $expr")
                }
            }

            else -> throw IllegalStateException("Unsupported logical expression: $expr")
        }
}