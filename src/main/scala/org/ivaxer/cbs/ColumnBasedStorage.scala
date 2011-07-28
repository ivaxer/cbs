package org.ivaxer.cbs

import java.io.{File, RandomAccessFile, IOException, ByteArrayInputStream, ByteArrayOutputStream}
import java.io.{FileOutputStream, FileInputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode._
import scala.io.Source
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.math.min

import SevenZip.Compression.LZMA.{Encoder, Decoder}

import protos.Protos.Header


object ColumnSchema {
    def parseFrom(file: File): ColumnSchema = {
        val lines = Source.fromFile(file).getLines
        val row_size = lines.next().toInt
        return new ColumnSchema(row_size)
    }
}


class ColumnSchema(val row_size: Int) {
    val nil = 0

    override def toString(): String = {
        return "Row size: " + row_size
    }
}


class ColumnReader(file: String, schema: ColumnSchema) {
    val channel = new FileInputStream(file).getChannel()
    val buffer = channel.map(READ_ONLY, 0, channel.size)
    var header: Header = null
    var data_start_offset = 0
    var data_end_offset = 0
    var row_start = 0
    var row_end = 0

    next_block()

    def next_block(): Header = {
        seek_to_next_header();
        header = read_header()
        if (header.hasCompressedBlockSize())
            throw new Exception("Not Implemented")
        data_start_offset = buffer.position()
        data_end_offset = data_start_offset + schema.row_size * header.getNumRows().toInt
        row_start = row_end
        row_end = row_start + header.getNumRows().toInt
        return header
    }

    def read(start: Int, count: Int): ByteBuffer = {
        if (start < row_start)
            throw new Exception("Out of range: start < first row in block")
        if (start + count > row_end)
            throw new Exception("Out of range: start + count >= last row in block")
        val length = count * schema.row_size
        val offset = data_start_offset + (start - row_start) * schema.row_size
        buffer.position(offset)
        val result = buffer.slice()
        result.limit(length)
        return result
    }

    def reset() = {
        buffer.position(0)
        data_start_offset = 0
        data_end_offset = 0
        row_start = 0
        row_end = 0
        next_block()
    }

    override def toString(): String = {
        return "Schema: " + schema.toString +
        "\nRow start: " + row_start +
        "\nRow end: " + row_end +
        "\nData start offset: " + data_start_offset +
        "\nData end offset: " + data_end_offset +
        "\nBacked buffer: " + buffer.toString +
        "\nHeader: " + header.toString
    }

    protected def seek_to_next_header() = {
        buffer.position(data_end_offset)
    }

    protected def read_header(): Header = {
        val header_size = buffer.get()
        val header_data = new Array[Byte](header_size)
        buffer.get(header_data)
        return Header.parseFrom(header_data)
    }
}


class ColumnWriter(file: String, schema: ColumnSchema) {
    val channel = new FileOutputStream(file, true).getChannel()

    def append(header: Header, data: ByteBuffer) = {
        write_header(header)
        channel.write(data)
    }

    def append(header: Header) {
        write_header(header)
    }


    override def toString(): String = {
        return "Schema: " + schema.toString +
        "\nFile channel: " + channel.toString
    }

    protected def write_header(header: Header) = {
        val data = header.toByteArray
        val header_size = data.length.toByte
        val buf = ByteBuffer.allocate(header_size + 1)
        buf.put(header_size)
        buf.put(data)
        buf.flip()
        channel.write(buf)
    }
}


class StorageError(msg: String) extends Exception


object OpenMode extends Enumeration("Read", "Write") {
    type OpenMode = Value
    val READ, WRITE = Value
}
import OpenMode._


class Storage(basedir: String, mode: OpenMode) {
    val schema_suffix = ".schema"

    val db = new File(basedir)

    if (!db.exists)
        throw new IOException("Storage directory '%s' does not exists" format basedir)

    val column_readers = new HashMap[String, ColumnReader]
    val column_writers = new HashMap[String, ColumnWriter]

    val schemas = new HashMap[String, ColumnSchema]
    val data_files = new HashMap[String, String]

    for (f <- db.listFiles().filter(_.getName.endsWith(schema_suffix))) {
        val name = f.getName().dropRight(schema_suffix.length)
        val fname = f.getAbsolutePath().dropRight(schema_suffix.length)

        schemas += name -> ColumnSchema.parseFrom(f)
        data_files += name -> fname
    }

    val encoder = build_encoder()
    val decoder = build_decoder()


    // XXX: return value is ListBuffer of ByteBuffers now.
    def read(column: String, start: Int, count: Int): ListBuffer[ByteBuffer] = {
        is_column_exists(column)
        val reader = get_column_reader(column)
        // XXX: read from start of file each call
        reader.reset()

        while (reader.row_end <= start) {
            reader.next_block()
        }

        val result = ListBuffer[ByteBuffer]()
        var pending_rows = count
        var cur_start = start

        while (pending_rows > 0) {
            val cur_count = min(pending_rows, reader.header.getNumRows().toInt)
            val buf = reader.read(cur_start, cur_count)
            result += buf
            pending_rows -= cur_count
            cur_start += cur_count
            if (pending_rows > 0)
                reader.next_block()
        }

        return result
    }

    def read(column: String, start: Int): ListBuffer[ByteBuffer] = read(column, start, 1)

    def append(column: String, header: Header, data: ByteBuffer) {
        is_column_exists(column)
        get_column_writer(column).append(header, data)
    }

    def append(column: String, header: Header) {
        is_column_exists(column)
        get_column_writer(column).append(header)
    }

    def append(column: String, data: ByteBuffer, compress: Boolean = false) {
        is_column_exists(column)
        val schema = schemas(column)
        val block_size = data.remaining
        val num_rows = block_size / schema.row_size
        val header_builder = Header.newBuilder().setNumRows(num_rows)
        var block_data: ByteBuffer = null
        if (is_empty(schema, data))
            header_builder.setBlockSize(0)
        else if (compress) {
            block_data = compress_data(data)
            header_builder.setCompressedBlockSize(block_data.remaining).setBlockSize(block_size)
        }
        else {
            block_data = data
            header_builder.setBlockSize(block_size)
        }
        val header = header_builder.build()
        println("Appended column '" + column + "': " + header)
        if (block_data != null)
            append(column, header, block_data)
        else
            append(column, header)
    }

    def append(columns: HashMap[String, ByteBuffer]) {
        append(columns, false)
    }

    def append(columns: HashMap[String, ByteBuffer], compress: Boolean) {
        for ((column, data) <- columns)
            append(column, data, compress)
    }

    def repack() = {
    }

    protected def compress_data(data: ByteBuffer): ByteBuffer = {
        val in = new ByteArrayInputStream(to_array(data))
        val out = new ByteArrayOutputStream()
        encoder.WriteCoderProperties(out)
        encoder.Code(in, out, -1, -1, null)
        ByteBuffer.wrap(out.toByteArray)
    }

    protected def decompress_data(compressed: ByteBuffer, uncompressed_size: Int): ByteBuffer = {
        val props = new Array[Byte](Encoder.kPropSize)
        compressed.get(props)
        val rest = compressed.slice()
        if (!decoder.SetDecoderProperties(props))
            throw new Exception("Incorrect coder properties")
        val in = new ByteArrayInputStream(to_array(rest))
        val out = new ByteArrayOutputStream(uncompressed_size)
        if (!decoder.Code(in, out, uncompressed_size))
            throw new Exception("Error in data stream")
        ByteBuffer.wrap(out.toByteArray)
    }

    protected def is_column_exists(column: String) = {
        if (!schemas.contains(column))
            throw new StorageError("Unknown column")
    }

    protected def get_column_reader(column: String): ColumnReader = {
        if (column_readers.contains(column)) {
            return column_readers(column)
        }
        else {
            val file = data_files(column)
            val schema = schemas(column)
            return new ColumnReader(file, schema)
        }
    }

    protected def get_column_writer(column: String): ColumnWriter = {
        if (column_writers.contains(column)) {
            return column_writers(column)
        }
        else {
            val file = data_files(column)
            val schema = schemas(column)
            return new ColumnWriter(file, schema)
        }
    }

    protected def build_encoder(): Encoder = {
        new Encoder()
    }

    protected def build_decoder(): Decoder = {
        new Decoder()
    }

    protected def to_array(data: ByteBuffer): Array[Byte] = {
        // XXX: memory coping:
        val buf = new Array[Byte](data.remaining)
        data.get(buf)
        buf
    }

    protected def is_empty(schema: ColumnSchema, data: ByteBuffer): Boolean = {
        val view = data.asLongBuffer()
        while (view.hasRemaining)
            if (view.get() != schema.nil)
                return false
        return true
    }
}

object ColumnBasedStorage {
    def main(args: Array[String]) = {
        val storage = new Storage("/tmp/storage", WRITE)
        val data1 = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16))
        storage.append("huj", data1)

        val data2 = ByteBuffer.wrap(Array[Byte](17, 18, 19, 20, 21, 22, 23, 24))
        storage.append("huj", data2)

        val rstorage = new Storage("/tmp/storage", READ)

        var row = new Array[Byte](8)
        for (buf <- rstorage.read("huj", 0, 3)) {
            while (buf.hasRemaining()) {
                buf.get(row)
                println(row.mkString)
            }
        }
    }
}

