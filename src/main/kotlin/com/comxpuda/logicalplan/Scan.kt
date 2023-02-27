package com.comxpuda.logicalplan

import com.comxpuda.datasource.DataSource
import com.comxpuda.datatypes.Schema


class Scan(
    val path: String,
    val dataSource: DataSource,
    val projection: List<String>
) : LogicalPlan {

    private val schema = deriveSchema()

    override fun schema(): Schema {
        return schema
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    private fun deriveSchema(): Schema {
        val schema = dataSource.schema()
        return if (projection.isEmpty()) {
            schema
        } else {
            schema.select(projection)
        }
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $path; projection=None"
        } else {
            "Scan: $path; projection=$projection"
        }
    }
}