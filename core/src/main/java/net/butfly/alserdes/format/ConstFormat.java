package net.butfly.alserdes.format;

import java.util.Map;

import net.butfly.alserdes.SerDes;

@SerDes.As("")
@SerDes.As("const")
public final class ConstFormat<M extends Map<String, Object>, D> extends Format<M, D> {
	private static final long serialVersionUID = 4665610987994353342L;

	@Override
	public M ser(M m) {
		return m;
	}

	@Override
	public M deser(M m) {
		return m;
	}
}
