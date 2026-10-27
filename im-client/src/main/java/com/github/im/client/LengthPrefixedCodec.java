package com.github.im.client;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * {@code 4 字节大端长度 + UTF-8 JSON} 的帧编解码，与网关的
 * {@code LengthFieldBasedFrameDecoder(maxFrameBytes, 0, 4, 0, 4)} 严格对齐。
 *
 * <h2>为什么要长度前缀</h2>
 *
 * <p>TCP 是字节流，没有消息边界。用换行符当分隔符看起来更省 4 字节，
 * 但 JSON 里的字符串可以合法包含换行（{@code body} 里一个回车就够），
 * 那就得转义或扫描字符串内部状态——两件都比 4 字节贵得多。
 *
 * <h2>为什么必须有长度上限</h2>
 *
 * <p>没有上限时，一个损坏或恶意的长度字段（比如 {@code 0x7FFFFFFF}）会让客户端
 * 直接按 2GB 分配缓冲区——这是经典的「一个整数打穿内存」。
 * 这里上限与服务端一致（默认 1MiB），超限立刻断开而不是继续读。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
final class LengthPrefixedCodec {

    private LengthPrefixedCodec() {
    }

    static byte[] encode(String json) {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[4 + payload.length];
        frame[0] = (byte) (payload.length >>> 24);
        frame[1] = (byte) (payload.length >>> 16);
        frame[2] = (byte) (payload.length >>> 8);
        frame[3] = (byte) payload.length;
        System.arraycopy(payload, 0, frame, 4, payload.length);
        return frame;
    }

    static void write(OutputStream out, String json) throws IOException {
        out.write(encode(json));
        out.flush();
    }

    /**
     * 读一帧。
     *
     * @return 帧内容；流已正常结束（对端关闭）时返回 {@code null}
     * @throws IOException      读失败、或长度字段超出 {@code maxBytes}
     * @throws EOFException     帧头/帧体读到一半就断了（说明连接在传输中被打断，必须重连）
     */
    static String read(InputStream in, int maxBytes) throws IOException {
        int b1 = in.read();
        if (b1 < 0) {
            return null;
        }
        byte[] header = new byte[4];
        header[0] = (byte) b1;
        readFully(in, header, 1, 3);
        int length = ((header[0] & 0xFF) << 24)
                | ((header[1] & 0xFF) << 16)
                | ((header[2] & 0xFF) << 8)
                | (header[3] & 0xFF);
        if (length < 0) {
            throw new IOException("帧长度非法：" + length);
        }
        if (length > maxBytes) {
            throw new IOException("帧超过上限：length=" + length + " max=" + maxBytes);
        }
        if (length == 0) {
            return "";
        }
        byte[] payload = new byte[length];
        readFully(in, payload, 0, length);
        return new String(payload, StandardCharsets.UTF_8);
    }

    private static void readFully(InputStream in, byte[] buffer, int offset, int length) throws IOException {
        int read = 0;
        while (read < length) {
            int n = in.read(buffer, offset + read, length - read);
            if (n < 0) {
                throw new EOFException("连接在帧中途关闭（已读 " + read + '/' + length + " 字节）");
            }
            read += n;
        }
    }
}
