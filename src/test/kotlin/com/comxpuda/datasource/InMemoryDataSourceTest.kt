package com.comxpuda.datasource

import com.comxpuda.datatypes.*
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.VarCharVector
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InMemoryDataSourceTest {

    @Test
    fun `scan in memory datasource`() {
        val idField = Field("id", ArrowTypes.Int16Type)
        val nameField = Field("name", ArrowTypes.StringType)
        val scoreField = Field("score", ArrowTypes.FloatType)
        val schema = Schema(listOf(idField, nameField, scoreField))

        val size = 10
        val idFieldVector = IntVector("id", RootAllocator(Long.MAX_VALUE))
        val nameFieldVector = VarCharVector("name", RootAllocator(Long.MAX_VALUE))
        val scoreFieldVector = Float4Vector("score", RootAllocator(Long.MAX_VALUE))
        idFieldVector.allocateNew(size)
        nameFieldVector.allocateNew(size)
        scoreFieldVector.allocateNew(size)
        idFieldVector.valueCount = size
        nameFieldVector.valueCount = size
        scoreFieldVector.valueCount = size

        val idBuilder = ArrowVectorBuilder(idFieldVector)
        val nameBuilder = ArrowVectorBuilder(nameFieldVector)
        val scoreBuilder = ArrowVectorBuilder(scoreFieldVector)

        (0 until size).forEach { idBuilder.set(it, it) }
        (0 until size).forEach { nameBuilder.set(it, it) }
        (0 until size).forEach { scoreBuilder.set(it, it) }

        val idColumnVector = idBuilder.build()
        val nameColumnVector = nameBuilder.build()
        val scoreColumnVector = scoreBuilder.build()

        val dataSource = InMemoryDataSource(
            schema,
            listOf(RecordBatch(schema, listOf(idColumnVector, nameColumnVector, scoreColumnVector)))
        )

        val sequence = dataSource.scan(listOf("score"))

        sequence.forEach { println(it) }
    }
}