package net.butfly.albacore.serder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import net.butfly.albacore.utils.Reflections;

public class JavaSerder implements BinarySerder<Object>, ClassInfoSerder<Object, byte[]> {
	private static final long serialVersionUID = 2446148201514088203L;

	public JavaSerder() {
		super();
	}

	@Override
	@SafeVarargs
	public final Object[] der(byte[] from, Class<?>... tos) {
		return (Object[]) der(from);
	}

	@Override
	public byte[] ser(Object from) {
		return toBytes(from);
	}

	@Override
	public <T> T der(byte[] from) {
		return fromBytes(from);
	}

	@SuppressWarnings("unchecked")
	public <T> T der(InputStream from) throws IOException {
		try (ObjectInputStream ois = Reflections.wrap(from, ObjectInputStream.class);) {
			return (T) ois.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		return der(from);
	}

	@Override
	public <T> T der(InputStream from, Class<T> to) throws IOException {
		return der(from);
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromBytes(byte[] b) {
		try (InputStream in = new ByteArrayInputStream(b); ObjectInputStream s = new ObjectInputStream(in);) {
			return (T) s.readObject();
		} catch (IOException | ClassNotFoundException e) {
			return null;
		}
	}

	public static <T> byte[] toBytes(T o) {
		try (ByteArrayOutputStream os = new ByteArrayOutputStream(); ObjectOutputStream s = new ObjectOutputStream(os);) {
			s.writeObject(o);
			return os.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}
}
