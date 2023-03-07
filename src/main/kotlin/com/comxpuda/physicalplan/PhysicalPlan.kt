package com.comxpuda.physicalplan

import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema

interface PhysicalPlan {

    fun schema(): Schema
    fun execute(): Sequence<RecordBatch>
    fun children(): List<PhysicalPlan>

    fun pretty(): String {
        return format(this)
    }

}

/** Format a logical plan in human-readable form */
fun format(plan: PhysicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent + 1)) }
    return b.toString()
}
