
package net.butfly.alserder;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

/**
 * Serializer and Deserializer
 * 
 * @param <FROM>
 * @param <TO>
 */
public interface SerDes<FROM, TO> extends Serializable {
	TO ser(FROM v);

	FROM der(TO r);

	@SuppressWarnings("unchecked")
	default TO sers(FROM... v) {
		return sers(Colls.list(v));
	}

	default TO sers(List<FROM> vs) {
		throw new UnsupportedOperationException();
	}

	default List<FROM> ders(TO r) {
		return Colls.list(der(r));
	}

	default SerDes<TO, FROM> revert() {
		return new SerDes<TO, FROM>() {
			private static final long serialVersionUID = 1247776407568101791L;

			@Override
			public FROM ser(TO v) {
				return SerDes.this.der(v);
			}

			@Override
			public TO der(FROM r) {
				return SerDes.this.ser(r);
			}

			@Override
			public Class<TO> fromClass() {
				return SerDes.this.toClass();
			}

			@Override
			public Class<FROM> toClass() {
				return SerDes.this.fromClass();
			}

			@Override
			public String format() {
				return "rev:" + SerDes.this.format();
			}
		};
	}

	// lookup
	static final String DEFAULT_SD_FORMAT = Configs.gets("alserder.default.format", "str");
	@SuppressWarnings("rawtypes")
	static final Map<String, SerDes> ALL_SDERS = Maps.of();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <F, T> SerDes<F, T> sd(String format) {
		boolean revert = format.startsWith("rev:");
		SerDes r = ALL_SDERS.computeIfAbsent(revert ? format.substring(4) : format, f -> {
			Set<Class<? extends SerDes>> impls = Reflections.getSubClasses(SerDes.class);
			for (Class<? extends SerDes> c : impls) {
				SerDesAs sd = c.getAnnotation(SerDesAs.class);
				if (null != sd && f.equals(sd.format())) try {
					return c.getConstructor().newInstance();
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e) {
							continue;
						}
			}
			return null;
		});
		return null == r || !revert ? r : r.revert();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	@interface SerDesAs { // default value for bson
		String format();

		Class<?> from() default Map.class;

		Class<?> to() default byte[].class;
	}

	// then
	default <THEN> SerDes<FROM, THEN> then(SerDes<TO, THEN> then) {
		return new SerDes<FROM, THEN>() {
			private static final long serialVersionUID = -1388017778891200080L;

			@Override
			public THEN ser(FROM o) {
				return then.ser(SerDes.this.ser(o));
			}

			@Override
			public FROM der(THEN r) {
				return SerDes.this.der(then.der(r));
			}

			@SuppressWarnings("unchecked")
			@Override
			public THEN sers(FROM... vs) {
				return then.ser(SerDes.this.sers(vs));
			}

			@Override
			public THEN sers(List<FROM> vs) {
				return then.ser(SerDes.this.sers(vs));
			}

			@Override
			public List<FROM> ders(THEN r) {
				List<FROM> f = Colls.list();
				then.ders(r).forEach(t -> f.add(SerDes.this.der(t)));
				return f;
			}
		};
	}

	// other utils
	class _Private {
		@SuppressWarnings("rawtypes")
		private static SerDesAs sdAnn(Class<? extends SerDes> cls) {
			Class<?> c = cls;
			SerDesAs ann = null;
			while (null == (ann = c.getAnnotation(SerDesAs.class)) && (null != (c = c.getSuperclass())));
			return ann;
		}
	}

	@SuppressWarnings("unchecked")
	default Class<FROM> fromClass() {
		return (Class<FROM>) _Private.sdAnn(this.getClass()).from();
	}

	@SuppressWarnings("unchecked")
	default Class<TO> toClass() {
		return (Class<TO>) _Private.sdAnn(this.getClass()).to();
	}

	default String format() {
		return _Private.sdAnn(this.getClass()).format();
	}
}
