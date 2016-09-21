package net.butfly.albacore.serder;

import java.util.HashMap;
import java.util.Map;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.MapSerder.SerderMap;

public class MapSerder extends BeanSerderBase<SerderMap> implements BeanSerder<SerderMap> {
	private static final long serialVersionUID = 7252864504025324603L;

	@Override
	public <T> SerderMap ser(T from) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T der(SerderMap from, TypeToken<T> to) {
		// TODO Auto-generated method stub
		return null;
	}

	public static final class SerderMap extends HashMap<CharSequence, Object> implements Map<CharSequence, Object> {
		private static final long serialVersionUID = -845704145309181433L;

		public SerderMap() {
			super();
		}

		public SerderMap(int initialCapacity, float loadFactor) {
			super(initialCapacity, loadFactor);
		}

		public SerderMap(int initialCapacity) {
			super(initialCapacity);
		}

		public SerderMap(Map<? extends CharSequence, ?> m) {
			super(m);
		}

		public static SerderMap from(Map<? extends CharSequence, ?> map) {
			return new SerderMap(map);
		}
	}
}