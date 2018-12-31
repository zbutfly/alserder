package net.butfly.alserder;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.utils.Generics;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Loggable;

public interface SerDes<FROM, TO> extends Serializable, Loggable {
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface AsList {
		As[] value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@Repeatable(AsList.class)
	public @interface As {
		String value();

		boolean list() default false;
	}

	@SuppressWarnings("deprecation")
	default Class<?> rawClass() {
		return Generics.getGenericParamClass(getClass(), SerDes.class, "TO");
	}

	default TO ser(FROM v) {
		throw new UnsupportedOperationException();
	}

	default FROM deser(TO r) {
		throw new UnsupportedOperationException();
	};

	default TO sers(List<FROM> l) {
		if (null == l || l.isEmpty()) return null;
		if (l.size() > 1) //
			logger().warn(getClass() + " does not support multiple serializing, only first will be writen: \n\t" + l.toString());
		FROM first = l.get(0);
		return null == first ? null : ser(first);
	}

	default List<FROM> desers(TO r) {
		return null == r ? Colls.list() : Colls.list(deser(r));
	}

	// trans
	default <THEN> SerDes<FROM, THEN> then(SerDes<TO, THEN> then) {
		return new SerDes<FROM, THEN>() {
			private static final long serialVersionUID = -1388017778891200080L;

			@Override
			public THEN ser(FROM o) {
				return then.ser(SerDes.this.ser(o));
			}

			@Override
			public FROM deser(THEN r) {
				return SerDes.this.deser(then.deser(r));
			}
		};
	}

	default <BEFORE> SerDes<BEFORE, TO> prior(SerDes<BEFORE, FROM> before) {
		return new SerDes<BEFORE, TO>() {
			private static final long serialVersionUID = -1388017778891200080L;

			@Override
			public TO ser(BEFORE o) {
				return SerDes.this.ser(before.ser(o));
			}

			@Override
			public BEFORE deser(TO r) {
				return before.deser(SerDes.this.deser(r));
			}
		};
	}

	default SerDes<TO, FROM> revert() {
		return new SerDes<TO, FROM>() {
			private static final long serialVersionUID = 1247776407568101791L;

			@Override
			public FROM ser(TO v) {
				return SerDes.this.deser(v);
			}

			@Override
			public TO deser(FROM r) {
				return SerDes.this.ser(r);
			}
		};
	}

	// other
	interface MapSerDes<TO> extends SerDes<Map<String, Object>, TO> {}
}
