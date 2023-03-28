package com.comxpuda.datatypes

class RecordBatch(val schema: Schema, val fields: List<ColumnVector>) {

    fun rowCount() = fields.first().size()

    fun columnCount() = fields.size

    /** Access one column by index */
    fun field(i: Int): ColumnVector {
        return fields[i]
    }

    /** Useful for testing */
    fun toCSV(): String {
        val b = StringBuilder()
        val columnCount = schema.fields.size

        (0 until rowCount()).forEach { rowIndex ->
            (fields.indices).forEach { columnIndex ->
                if (columnIndex > 0) {
                    b.append(",")
                }
                val v = fields[columnIndex]
                val value = v.getValue(rowIndex)
                if (value == null) {
                    b.append("null")
                } else if (value is ByteArray) {
                    b.append(String(value))
                } else {
                    b.append(value)
                }
            }
            b.append("\n")
        }
        return b.toString()
    }

    override fun toString(): String {
        return toCSV()
    }

}