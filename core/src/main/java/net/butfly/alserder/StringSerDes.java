package net.butfly.alserder;

@SerAs(format = "str", from = String.class, to = byte[].class)
public class StringSerDes implements SerDes<String, byte[]> {
	private static final long serialVersionUID = 6541727872025508373L;

	@Override
	public byte[] ser(String v) {
		return null == v ? null : v.getBytes();
	}

	@Override
	public String deser(byte[] v) {
		return null == v ? null : new String(v);
	}
}
