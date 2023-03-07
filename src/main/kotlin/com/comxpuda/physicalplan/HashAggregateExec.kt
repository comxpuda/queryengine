package com.comxpuda.physicalplan

import com.comxpuda.datatypes.ArrowFieldVector
import com.comxpuda.datatypes.ArrowVectorBuilder
import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot

class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: Schema
) : PhysicalPlan {
    override fun schema(): Schema {
        return schema
    }

    override fun execute(): Sequence<RecordBatch> {
        val map = HashMap<List<Any?>, List<Accumulator>>()

        // for each batch from the input executor
        input.execute().iterator().forEach { batch ->
            // evaluate the grouping expressions
            val groupKeys = groupExpr.map { it.evaluate(batch) }
            // evaluate the expressions that are inputs to the aggregate functions
            val aggrInputValues = aggregateExpr.map { it.inputExpression().evaluate(batch) }

            // for each row in the batch
            (0 until batch.rowCount()).forEach { rowIndex ->
                // create the key for the hash map
                val rowKey =
                    groupKeys.map {
                        val value = it.getValue(rowIndex)
                        when (value) {
                            is ByteArray -> String(value)
                            else -> value
                        }
                    }

                // println(rowKey)
                // get or create accumulators for this grouping key
                val accumulators = map.getOrPut(rowKey) { aggregateExpr.map { it.createAccumulator() } }

                // perform accumulation
                accumulators.withIndex().forEach { accum ->
                    val value = aggrInputValues[accum.index].getValue(rowIndex)
                    accum.value.accumulate(value)
                }
            }
        }
        // create result batch containing final aggregate values
        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = map.size

        val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

        map.entries.withIndex().forEach { entry ->
            val rowIndex = entry.index
            val groupingKey = entry.value.key
            val accumulators = entry.value.value
            groupExpr.indices.forEach { builders[it].set(rowIndex, groupingKey[it]) }
            aggregateExpr.indices.forEach {
                builders[groupExpr.size + it].set(rowIndex, accumulators[it].finalValue())
            }
        }

        val outputBatch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
        // println("HashAggregateExec output:\n${outputBatch.toCSV()}")
        return listOf(outputBatch).asSequence()

    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "HashAggregateExec: groupExpr=$groupExpr, aggrExpr=$aggregateExpr"
    }
}