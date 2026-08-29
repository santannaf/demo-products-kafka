using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace DemoProducts.Infrastructure.Messaging.Kafka.Wire;

/// <summary>
/// The two Avro binary primitives this contract needs — a variable-length zig-zag integer and a
/// UTF-8 string — plus Confluent's five-byte framing.
/// </summary>
/// <remarks>
/// Hand-written rather than taken from <c>Apache.Avro</c> because that package reaches Avro's schema
/// parser, which is built on Newtonsoft.Json: 34 of the Api's 43 ILC warnings came from that one edge.
/// The scope of what is hand-written is deliberately tiny — this file understands <c>long</c> and
/// <c>string</c> and nothing else — and it is pinned by tests that read the bytes back with the real
/// Avro library, so "our encoder and Avro agree" is asserted rather than assumed. Anything richer than
/// a flat record of primitives should go back to <c>Apache.Avro</c> instead of growing this file.
/// </remarks>
internal static class AvroBinaryWriter
{
    /// <summary>Confluent's wire format: a zero byte, then the schema id as four big-endian bytes.</summary>
    private const byte MagicByte = 0x00;

    private static readonly UTF8Encoding Utf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>
    /// Frames an already-encoded record body so the Schema Registry deserializer on the other side can
    /// find the writer schema.
    /// </summary>
    public static byte[] Frame(int schemaId, ReadOnlySpan<byte> body)
    {
        var framed = new byte[5 + body.Length];

        framed[0] = MagicByte;
        BinaryPrimitives.WriteInt32BigEndian(framed.AsSpan(1, 4), schemaId);
        body.CopyTo(framed.AsSpan(5));

        return framed;
    }

    /// <summary>
    /// Writes an Avro <c>long</c> — the encoding Avro also uses for a string's length. Zig-zag maps the
    /// sign onto the low bit so small negatives stay short, then the result is a base-128 varint with the
    /// high bit marking "one more byte".
    /// </summary>
    public static void WriteLong(IBufferWriter<byte> writer, long value)
    {
        ArgumentNullException.ThrowIfNull(writer);

        var zigZag = (ulong)((value << 1) ^ (value >> 63));
        var span = writer.GetSpan(10);
        var written = 0;

        while (zigZag > 0x7F)
        {
            span[written++] = (byte)((zigZag & 0x7F) | 0x80);
            zigZag >>= 7;
        }

        span[written++] = (byte)zigZag;
        writer.Advance(written);
    }

    /// <summary>Writes an Avro <c>string</c>: its length in bytes, then its UTF-8 bytes.</summary>
    public static void WriteString(IBufferWriter<byte> writer, string value)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(value);

        var byteCount = Utf8.GetByteCount(value);
        WriteLong(writer, byteCount);

        if (byteCount == 0)
        {
            return;
        }

        var span = writer.GetSpan(byteCount);
        Utf8.GetBytes(value, span);
        writer.Advance(byteCount);
    }
}
