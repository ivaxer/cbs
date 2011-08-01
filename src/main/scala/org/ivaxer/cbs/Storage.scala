package org.ivaxer.cbs

import java.io.{File, FileWriter, IOException}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
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
    var compressed_block = false
    var row_start = 0
    var row_end = 0

    val decoder = new Decoder()

    // XXX: return value is ListBuffer of ByteBuffers now.
    def read(start: Int, count: Int): ListBuffer[ByteBuffer] = {
        assert(start >= 0)
        assert(count > 0)

        if (row_start > start)
            reset()

        while (row_end <= start)
            next_block()

        val result = new ListBuffer[ByteBuffer]
        var pending_rows = count
        var first_row = start

        while (pending_rows > 0) {
            val cur_count = min(pending_rows, row_end - first_row)
            val buf = read_block_rows(first_row, cur_count)
            result += buf
            pending_rows -= cur_count
            first_row += cur_count
            if (pending_rows > 0)
                next_block()
        }

        result
    }

    protected def next_block(): Header = {
        seek_to_next_header();
        header = read_header()
        if (header.hasBitmapSize || header.hasCompressedBitmapSize)
            throw new Exception("Not Implemented")
        compressed_block = header.hasCompressedBlockSize
        val data_size = {
            if (header.hasCompressedBlockSize())
                header.getCompressedBlockSize().toInt
            else
                header.getBlockSize().toInt
        }
        data_start_offset = buffer.position()
        data_end_offset = data_start_offset + data_size
        row_start = row_end
        row_end = row_start + header.getNumRows().toInt
        return header
    }

    protected def read_block_rows(start: Int, count: Int): ByteBuffer = {
        if (start < row_start)
            throw new Exception("Out of range: start < first row in block")
        if (start + count > row_end)
            throw new Exception("Out of range: start + count > last row in block")
        val data = get_block_data()
        val length = count * schema.row_size
        val offset = (start - row_start) * schema.row_size
        data.position(offset)
        val result = data.slice()
        result.limit(length)
        return result
    }

    protected def get_block_data(): ByteBuffer = {
        val compressed_size = header.getCompressedBlockSize().toInt
        val out_size = header.getBlockSize().toInt
        if (compressed_block) {
            buffer.position(data_start_offset)
            val coded_data = buffer.slice()
            coded_data.limit(compressed_size)
            return decode(coded_data, out_size)
        }
        else {
            buffer.position(data_start_offset)
            val data = buffer.slice()
            data.limit(out_size)
            return data
        }
    }

    protected def reset() {
        buffer.position(0)
        header = null
        data_start_offset = 0
        data_end_offset = 0
        row_start = 0
        row_end = 0
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

    protected def seek_to_next_header() {
        buffer.position(data_end_offset)
    }

    protected def read_header(): Header = {
        val header_size = buffer.get()
        val header_data = new Array[Byte](header_size)
        buffer.get(header_data)
        return Header.parseFrom(header_data)
    }

    protected def decode(data: ByteBuffer, out_size: Int): ByteBuffer = {
        val props = new Array[Byte](Encoder.kPropSize)
        data.get(props)
        val rest = data.slice()
        if (!decoder.SetDecoderProperties(props))
            throw new Exception("Incorrect coder properties")
        val in = new ByteArrayInputStream(to_array(rest))
        val out = new ByteArrayOutputStream(out_size)
        if (!decoder.Code(in, out, out_size))
            throw new Exception("Error in data stream")
        ByteBuffer.wrap(out.toByteArray)
    }

    protected def to_array(data: ByteBuffer): Array[Byte] = {
        // XXX: memory coping:
        val buf = new Array[Byte](data.remaining)
        data.get(buf)
        buf
    }
}


class ColumnWriter(file: String, schema: ColumnSchema) {
    val channel = new FileOutputStream(file, true).getChannel()
    val compress = true

    val encoder = build_encoder()

    def append(header: Header, data: ByteBuffer) {
        write_header(header)
        channel.write(data)
    }

    def append(header: Header) {
        write_header(header)
    }

    def append(data: ByteBuffer) {
        val block_size = data.remaining
        val num_rows = block_size / schema.row_size
        val header_builder = Header.newBuilder().setNumRows(num_rows)
        var block_data: ByteBuffer = null
        if (is_empty(schema, data))
            header_builder.setBlockSize(0)
        else if (compress) {
            block_data = encode(data)
            header_builder.setCompressedBlockSize(block_data.remaining).setBlockSize(block_size)
        }
        else {
            block_data = data
            header_builder.setBlockSize(block_size)
        }
        val header = header_builder.build()
        println("Appended block to '%s':\nHeader = {\n%s}\n" format(file, header))
        if (block_data != null)
            append(header, block_data)
        else
            append(header)
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

    protected def build_encoder(): Encoder = {
        new Encoder()
    }

    protected def encode(data: ByteBuffer): ByteBuffer = {
        val in = new ByteArrayInputStream(to_array(data))
        val out = new ByteArrayOutputStream()
        encoder.WriteCoderProperties(out)
        encoder.Code(in, out, -1, -1, null)
        ByteBuffer.wrap(out.toByteArray)
    }

    protected def is_empty(schema: ColumnSchema, data: ByteBuffer): Boolean = {
        val view = data.asLongBuffer()
        while (view.hasRemaining)
            if (view.get() != schema.nil)
                return false
        return true
    }

    protected def to_array(data: ByteBuffer): Array[Byte] = {
        // XXX: memory coping:
        val buf = new Array[Byte](data.remaining)
        data.get(buf)
        buf
    }
}


class StorageException(msg: String) extends Exception(msg: String)


class StorageIOException(msg: String) extends StorageException(msg: String)


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

    // XXX: return value is ListBuffer of ByteBuffers now.
    def read(column: String, start: Int, count: Int): ListBuffer[ByteBuffer] = {
        get_column_reader(column).read(start, count)
    }

    def read(column: String, start: Int): ListBuffer[ByteBuffer] = read(column, start, 1)

    def append(column: String, header: Header, data: ByteBuffer) {
        get_column_writer(column).append(header, data)
    }

    def append(column: String, header: Header) {
        get_column_writer(column).append(header)
    }

    def append(column: String, data: ByteBuffer) {
        get_column_writer(column).append(data)
    }

    def append(columns: HashMap[String, ByteBuffer]) {
        for ((column, data) <- columns)
            append(column, data)
    }

    def repack() = {
    }

    def create(column: String, schema: ColumnSchema) {
        val schema_file = new File(db, column + schema_suffix)
        if (schema_file.exists)
            throw new StorageIOException("Column schema '%s' already exists" format column)
        schema_file.createNewFile()
        val fw = new FileWriter(schema_file)
        fw.write("%s\n" format schema.row_size)
        fw.close()
        data_files += column -> new File(db, column).getAbsolutePath
        schemas += column -> schema
    }

    def create(columns: HashMap[String, ColumnSchema]) {
        for ((column, schema) <- columns)
            create(column, schema)
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
}

