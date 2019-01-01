package net.butfly.alserder.format;

import java.util.Map;

import net.butfly.alserder.SerDes;

@SerDes.As("")
@SerDes.As("const")
public final class ConstFormat<M extends Map<String, Object>, D> extends Format<M, D> {
	private static final long serialVersionUID = 4665610987994353342L;

	@Override
	public M assemble(M m, @SuppressWarnings("unchecked") D... extra) {
		return m;
	}

	@Override
	public M disassemble(M m, @SuppressWarnings("unchecked") D... extra) {
		return m;
	}
}
