package com.comxpuda.physicalplan

import com.comxpuda.datasource.DataSource
import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema

class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {
    override fun schema(): Schema {
        return ds.schema().select(projection)
    }

    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection)
    }

    override fun children(): List<PhysicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return "ScanExec: schema=${schema()}, projection=$projection"
    }
}