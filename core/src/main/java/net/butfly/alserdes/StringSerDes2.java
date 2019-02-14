package net.butfly.alserdes;

@SerDes.As("str_zs_kafka")
public class StringSerDes2 implements SerDes<String, String> {
	private static final long serialVersionUID = -7820370306233528192L;

	@Override
	public String ser(String v) {
		return null == v ? null : v.substring(1, v.length()-1);
	}

	@Override
	public String deser(String v) {
		return null == v ? null : new String(v);
	}
}
