package no.ssb.lds.core.utils;

import java.io.InputStream;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileAndClasspathReaderUtils {

    public static Path currentPath() {
        return Paths.get(".").toAbsolutePath().normalize();
    }

    public static String readFileOrClasspathResource(String path) {
        String utf8Str;
        if (Files.exists(Paths.get(path))) {
            utf8Str = readFileAsUtf8(path);
        } else {
            utf8Str = getResourceAsString(path, StandardCharsets.UTF_8);
        }
        return utf8Str;
    }

    public static String getResourceAsString(String path, Charset charset) {
        try {
            URLConnection conn = ClassLoader.getSystemResource(path).openConnection();
            try (InputStream is = conn.getInputStream()) {
                byte[] bytes = is.readAllBytes();
                CharBuffer cbuf = CharBuffer.allocate(bytes.length);
                CoderResult coderResult = charset.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
                if (coderResult.isError()) {
                    coderResult.throwException();
                }
                return cbuf.flip().toString();
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String readFileAsUtf8(String pathStr) {
        try {
            Path path = Paths.get(pathStr);
            if (Files.notExists(path)) {
                throw new RuntimeException("File not found: " + pathStr);
            }
            byte[] bytes = Files.readAllBytes(path);
            CharBuffer cbuf = CharBuffer.allocate(bytes.length);
            CoderResult coderResult = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes), cbuf, true);
            if (coderResult.isError()) {
                coderResult.throwException();
            }
            return cbuf.flip().toString();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
