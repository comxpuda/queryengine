package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Schema

interface DataFrame {

    fun project(expr: List<LogicalExpr>): DataFrame

    fun filter(expr: LogicalExpr): DataFrame

    fun aggregate(groupBy: List<LogicalExpr>, aggregateExpr: List<AggregateExpr>): DataFrame

    fun schema(): Schema

    fun logicalPlan(): LogicalPlan

}

class DataFrameImpl(private val plan: LogicalPlan) : DataFrame {
    override fun project(expr: List<LogicalExpr>): DataFrame {
        return DataFrameImpl(Projection(plan, expr))
    }

    override fun filter(expr: LogicalExpr): DataFrame {
        return DataFrameImpl(Selection(plan, expr))
    }

    override fun aggregate(groupBy: List<LogicalExpr>, aggregateExpr: List<AggregateExpr>): DataFrame {
        return DataFrameImpl(Aggregate(plan, groupBy, aggregateExpr))
    }

    override fun schema(): Schema {
        return plan.schema()
    }

    override fun logicalPlan(): LogicalPlan {
        return plan
    }

}