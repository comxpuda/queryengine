package com.comxpuda.datasource

import com.comxpuda.datasource.ParquetTimestampUtils.getTimestampMillis
import com.comxpuda.datatypes.ArrowFieldVector
import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.RecordReader
import org.apache.parquet.schema.Type
import java.util.logging.Logger

class ParquetDataSource(private val filename: String) : DataSource {
    override fun schema(): Schema {
        return ParquetScan(filename, listOf()).use {
            val arrowSchema = SchemaConverter(true).fromParquet(it.schema).arrowSchema
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
        return ParquetIterator(reader, columns, 1024)
    }

}

class ParquetIterator(
    private val reader: ParquetFileReader,
    private val projectedColumns: List<String>,
    private val batchSize: Int
) :
    Iterator<RecordBatch> {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter(true).fromParquet(schema).arrowSchema

    val projectedSchema =
        org.apache.arrow.vector.types.pojo.Schema(projectedColumns.map { name -> arrowSchema.fields.find { it.name == name } })

    var batch: RecordBatch? = null

    var pages: PageReadStore? = null

    lateinit var recordReader: RecordReader<*>

    var offset = 0

    override fun hasNext(): Boolean {
        if (offset == 0) {
            pages = reader.readNextRowGroup()
            val pSchema = reader.footer.fileMetaData.schema
            val columnIO = ColumnIOFactory().getColumnIO(pSchema)
            recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
        } else {
            return offset < pages!!.rowCount
        }
        return pages != null
    }

    override fun next(): RecordBatch {
        val rows = pages!!.rowCount

        var simpleGroups = mutableListOf<SimpleGroup>()
        var count = batchSize
        var start = offset
        while (start++ < rows && count-- > 0) {
            val simpleGroup = recordReader.read() as SimpleGroup
            simpleGroups.add(simpleGroup)
        }
        //        reader.close();
        val pSchema = reader.footer.fileMetaData.schema
        val fields = pSchema.fields
        val next = createBatch(Parquet(simpleGroups, fields))
        batch = null
        offset += simpleGroups.size
        return next!!
    }

    private fun createBatch(parquetData: Parquet): RecordBatch? {
        val rows = parquetData.data
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

                is TimeStampNanoVector ->
                    rows.withIndex().forEach { row ->
                        val int96Buf = row.value.getInt96(field.value.name, 0)
                        vector.setSafe(row.index, getTimestampMillis(int96Buf))
                    }

                is VarBinaryVector ->
                    rows.withIndex().forEach { row ->
                        vector.setSafe(row.index, row.value.getBinary(field.value.name, 0).bytes)
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

data class Parquet(val data: List<SimpleGroup>, val schema: List<Type>)