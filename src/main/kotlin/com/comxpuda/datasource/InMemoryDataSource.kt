package com.comxpuda.datasource

import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema

class InMemoryDataSource(val schema: Schema, val data: List<RecordBatch>) : DataSource {
    override fun schema(): Schema {
        return schema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        val projectionIndices = projection.map { name -> schema.fields.indexOfFirst { it.name == name } }
        return data.asSequence().map { batch ->
            RecordBatch(schema, projectionIndices.map { i -> batch.field(i) })
        }
    }
}