package com.comxpuda.datasource

import com.comxpuda.datatypes.ArrowFieldVector
import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.text.SimpleDateFormat
import java.util.logging.Logger

class ParquetDataSource(private val filename: String) : DataSource {
    override fun schema(): Schema {
        return ParquetScan(filename, listOf()).use {
            val arrowSchema = SchemaConverter().fromParquet(it.schema).arrowSchema
            com.comxpuda.datatypes.SchemaConverter.fromArrow(arrowSchema)
        }
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        return ParquetScan(filename, projection)
    }

}

class ParquetScan(filename: String, private val columns: List<String>) : AutoCloseable, Sequence<RecordBatch> {

    private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))

    val schema = reader.footer.fileMetaData.schema

    override fun close() {
        reader.close()
    }

    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader, columns)
    }

}

class ParquetIterator(private val reader: ParquetFileReader, private val projectedColumns: List<String>) :
    Iterator<RecordBatch> {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    val projectedSchema =
        org.apache.arrow.vector.types.pojo.Schema(projectedColumns.map { name -> arrowSchema.fields.find { it.name == name } })

    var batch: RecordBatch? = null

    override fun hasNext(): Boolean {
        batch = nextBatch()
        return batch != null
    }

    override fun next(): RecordBatch {
        val next = batch
        batch = null
        return next!!
    }

    private fun nextBatch(): RecordBatch? {
        val parquetData = ParquetReaderUtils.getParquetData(reader)
        val rows = parquetData.getData()
        if (rows.isEmpty()) {
            return null
        }

        val root = VectorSchemaRoot.create(projectedSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        val qSchema = com.comxpuda.datatypes.SchemaConverter.fromArrow(projectedSchema)
        root.fieldVectors.withIndex().forEach { field ->
            val vector = field.value
            when (vector) {
                is BitVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(
                            row.index,
                            row.value.getBoolean(field.value.name, 0).let { if (it) 0x01 else 0x00 })
                    }

                is VarBinaryVector ->
                    rows.withIndex().forEach { row ->
                        try {
                            vector.setSafe(row.index, row.value.getString(field.value.name, 0).toByteArray())
                        } catch (e: ClassCastException) {
                            // fixme how to fix timestamp handle
                            val timestampBytes = row.value.getInt96(field.value.name, 0).bytes
                            val buf: ByteBuffer = ByteBuffer.wrap(timestampBytes)
                            buf.order(ByteOrder.LITTLE_ENDIAN)

                            val date =
                                SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(
                                    (NanoTime(buf.int, buf.long).timeOfDayNanos)
                                )
                            vector.setSafe(row.index, date.toByteArray())
                        }
                    }

                is VarCharVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getBinary(field.value.name, 0).bytes)
                    }

                is TinyIntVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getInteger(field.value.name, 0))
                    }

                is SmallIntVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getInteger(field.value.name, 0))
                    }

                is IntVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getInteger(field.value.name, 0))
                    }

                is BigIntVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getLong(field.value.name, 0))
                    }

                is Float4Vector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getFloat(field.value.name, 0))
                    }

                is Float8Vector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getDouble(field.value.name, 0))
                    }

                else ->
                    throw IllegalStateException("No support for reading Parquet columns with data type $vector")
            }
            field.value.valueCount = rows.size
        }
        return RecordBatch(qSchema, root.fieldVectors.map { ArrowFieldVector(it) })
    }

}