package net.butfly.alserdes.avro;

import java.util.Map;

public class AvroSerDesTest {
	public static void main(String... args) {
		byte[] b = new byte[] { 16, 122, 104, 97, 110, 103, 115, 97, 110, 2 };
		Map<String, Object> m = new AvroSerDes().deser(b);
		System.out.println(m);
	}
}
