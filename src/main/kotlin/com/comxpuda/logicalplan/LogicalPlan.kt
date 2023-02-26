package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Schema

interface LogicalPlan {
    fun schema(): Schema

    fun children(): List<LogicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

fun format(plan: LogicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent + 1)) }
    return b.toString()
}