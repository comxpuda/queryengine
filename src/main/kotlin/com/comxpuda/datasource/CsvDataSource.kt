package com.comxpuda.datasource

import com.comxpuda.datatypes.*
import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import java.io.File
import java.io.FileNotFoundException
import java.util.logging.Logger

class CsvDataSource(
    val filename: String,
    val schema: Schema?,
    private val hasHeaders: Boolean,
    private val batchSize: Int
) : DataSource {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private val finalSchema: Schema by lazy { schema ?: inferSchema() }

    private fun inferSchema(): Schema {
        logger.info("inferSchema()")
        val file = File(filename)

        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }

        val parser = buildParser(defaultSettings())
        return file.inputStream().use {
            parser.beginParsing(it.reader())
            parser.detectedFormat

            parser.parseNext()

            val headers = parser.context.parsedHeaders().filterNotNull()

            val schema =
                if (hasHeaders) {
                    Schema(headers.map { colName -> Field(colName, ArrowTypes.StringType) })
                } else {
                    Schema(headers.mapIndexed { i, _ -> Field("field_${i + 1}", ArrowTypes.StringType) })
                }
            parser.stopParsing()
            schema
        }

    }

    private fun buildParser(defaultSettings: CsvParserSettings): CsvParser {
        return CsvParser(defaultSettings)
    }

    private fun defaultSettings(): CsvParserSettings {
        return CsvParserSettings().apply {
            isDelimiterDetectionEnabled = true
            isLineSeparatorDetectionEnabled = true
            skipEmptyLines = true
            isAutoClosingEnabled = true
        }
    }

    override fun schema(): Schema {
        return finalSchema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        logger.info("scan() projection=$projection")

        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }
        val readSchema =
            if (projection.isNotEmpty()) {
                finalSchema.select(projection)
            } else {
                finalSchema
            }

        val settings = defaultSettings()
        if (projection.isNotEmpty()) {
            settings.selectFields(*projection.toTypedArray())
        }

        settings.isHeaderExtractionEnabled = hasHeaders
        if (!hasHeaders) {
            settings.setHeaders(*readSchema.fields.map { it.name }.toTypedArray())
        }

        val parser = buildParser(settings)
        parser.beginParsing(file.inputStream().reader())
        parser.detectedFormat

        return ReaderAsSequence(readSchema, parser, batchSize)
    }
}

class ReaderAsSequence(val schema: Schema, val parser: CsvParser, val batchSize: Int) : Sequence<RecordBatch> {
    override fun iterator(): Iterator<RecordBatch> {
        return ReaderIterator(schema, parser, batchSize)
    }
}

class ReaderIterator(val schema: Schema, val parser: CsvParser, val batchSize: Int) : Iterator<RecordBatch> {

    private val logger = Logger.getLogger(CsvDataSource::class.simpleName)

    private var next: RecordBatch? = null
    private var started: Boolean = false

    override fun hasNext(): Boolean {
        if (!started) {
            started = true
            next = nextBatch()
        }

        return next != null
    }

    private fun nextBatch(): RecordBatch? {
        val rows = ArrayList<Record>(batchSize)
        do {
            val line = parser.parseNextRecord()
            if (line != null) {
                rows.add(line)
            }
        } while (line != null && rows.size < batchSize)
        if (rows.isEmpty()) {
            return null
        }
        return createBatch(rows)
    }

    override fun next(): RecordBatch {
        if (!started) {
            hasNext()
        }

        val out = next

        next = nextBatch()

        if (out == null) {
            throw NoSuchElementException(
                "Cannot read past the end of ${ReaderIterator::class.simpleName}"
            )
        }
        return out
    }

    private fun createBatch(rows: ArrayList<Record>): RecordBatch {
        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.fieldVectors.forEach { it.setInitialCapacity(rows.size) }
        root.allocateNew()

        root.fieldVectors.withIndex().forEach { field ->
            val vector = field.value
            when (vector) {
                is VarCharVector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        vector.setSafe(row.index, valueStr.toByteArray())
                    }

                is TinyIntVector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toByte())
                        }
                    }

                is SmallIntVector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toShort())
                        }
                    }

                is IntVector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toInt())
                        }
                    }

                is BigIntVector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toLong())
                        }
                    }

                is Float4Vector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "").trim()
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toFloat())
                        }
                    }

                is Float8Vector ->
                    rows.withIndex().forEach { row ->
                        val valueStr = row.value.getValue(field.value.name, "")
                        if (valueStr.isEmpty()) {
                            vector.setNull(row.index)
                        } else {
                            vector.set(row.index, valueStr.toDouble())
                        }
                    }

                else ->
                    throw IllegalStateException("No support for reading CSV columns with data type $vector")
            }
            field.value.valueCount = rows.size
        }
        return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

    }

}