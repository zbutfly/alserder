package net.butfly.alserder;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SerAs {
	String format();

	Class<?> from() default Map.class;

	Class<?> to() default byte[].class;

	// boolean byField() default true;
	// other utils
	class _Private {
		static final String DEFAULT_SD_FORMAT = Configs.gets("alserder.default.format", "str");
		@SuppressWarnings("rawtypes")
		static final Map<String, SerDes> ALL_SDERS = Maps.of();

		@SuppressWarnings("rawtypes")
		static SerAs sdAnn(Class<? extends SerDes> cls) {
			Class<?> c = cls;
			SerAs ann = null;
			while (null == (ann = c.getAnnotation(SerAs.class)) && (null != (c = c.getSuperclass())));
			return ann;
		}
	}
}
