import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

final class Protocol {
    static final byte PRODUCE = 1;
    static final byte FETCH = 2;
    static final byte CREATE_TOPIC = 3;
    static final byte METADATA = 4;
    static final byte OK = 0;
    static final byte INVALID_REQUEST = 1;
    static final byte SERVER_ERROR = 2;
    static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;
    private static final int MAX_STRING_SIZE = 1024 * 1024;

    static byte[] readFrame(DataInputStream input) throws IOException {
        int size = input.readInt();
        if (size < 1 || size > MAX_FRAME_SIZE) {
            throw new ProtocolException("invalid frame size: " + size);
        }
        byte[] frame = new byte[size];
        input.readFully(frame);
        return frame;
    }

    static void writeFrame(DataOutputStream output, byte[] frame) throws IOException {
        if (frame.length < 1 || frame.length > MAX_FRAME_SIZE) {
            throw new ProtocolException("invalid frame size: " + frame.length);
        }
        output.writeInt(frame.length);
        output.write(frame);
        output.flush();
    }

    static String readString(DataInputStream input) throws IOException {
        byte[] bytes = readBytes(input, false);
        if (bytes.length > MAX_STRING_SIZE) throw new ProtocolException("string is too large");
        return new String(bytes, StandardCharsets.UTF_8);
    }

    static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_SIZE) throw new ProtocolException("string is too large");
        writeBytes(output, bytes);
    }

    static byte[] readBytes(DataInputStream input, boolean nullable) throws IOException {
        int size = input.readInt();
        if (nullable && size == -1) return null;
        if (size < 0 || size > MAX_FRAME_SIZE) {
            throw new ProtocolException("invalid byte array size: " + size);
        }
        byte[] bytes = new byte[size];
        input.readFully(bytes);
        return bytes;
    }

    static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
        if (value.length > MAX_FRAME_SIZE) throw new ProtocolException("byte array is too large");
        output.writeInt(value.length);
        output.write(value);
    }

    static void writeNullableBytes(DataOutputStream output, byte[] value) throws IOException {
        if (value == null) {
            output.writeInt(-1);
        } else {
            writeBytes(output, value);
        }
    }

    static void requireEnd(DataInputStream input) throws IOException {
        if (input.available() != 0) throw new ProtocolException("unexpected trailing bytes");
    }

    static final class ProtocolException extends IOException {
        private static final long serialVersionUID = 1L;

        ProtocolException(String message) {
            super(message);
        }
    }

    private Protocol() {}
}
