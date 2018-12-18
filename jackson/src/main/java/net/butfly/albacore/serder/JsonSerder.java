package net.butfly.albacore.serder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.entity.ContentType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.serder.json.Jsons;
import net.butfly.albacore.serder.support.ContentSerderBase;
import net.butfly.albacore.utils.logger.Logger;

public class JsonSerder<E> extends ContentSerderBase implements TextSerder<E>, BeanSerder<E, CharSequence> {
	private static final long serialVersionUID = -4394900785541475884L;
	static final Logger logger = Logger.getLogger(JsonSerder.class);
	public static final JsonMapSerder JSON_MAPPER = new JsonMapSerder();

	public static <T> JsonSerder<T> SERDER(Class<T> cl) {
		return new JsonSerder<T>();
	}

	public JsonSerder() {
		this.contentType = ContentType.APPLICATION_JSON;
	}

	@Override
	public String ser(E from) {
		try {
			return Jsons.mapper.writeValueAsString(from);
		} catch (JsonProcessingException e) {
			throw new SystemException("", e);
		}
	}

	@Override
	public Object[] der(CharSequence from, Class<?>... tos) {
		JsonNode[] n;
		try {
			n = Jsons.array(Jsons.mapper.readTree(from.toString()));
		} catch (IOException e) {
			return null;
		}
		Object[] r = new Object[Math.min(tos.length, n.length)];
		for (int i = 0; i < r.length; i++)
			r[i] = single(n[i], tos[i]);
		return r;
	}

	protected <T> T single(JsonNode node, Class<T> tos) {
		try {
			return Jsons.mapper.readValue(Jsons.mapper.treeAsTokens(node), tos);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public <T extends E> T der(CharSequence from, Class<T> to) {
		if (null == from) return null;
		try {
			return Jsons.mapper.readValue(from.toString(), to);
		} catch (IOException e) {
			throw new RuntimeException("JSON der fail", e);
		}
	}

	@SuppressWarnings("unchecked")
	public static final class JsonMapSerder extends JsonSerder<Map<String, Object>> implements
			ClassInfoSerder<Map<String, Object>, CharSequence> {
		private static final long serialVersionUID = 6664350391207228363L;
		private static final JavaType t = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);
		private static final JavaType ts = TypeFactory.defaultInstance().constructParametricType(List.class, TypeFactory.defaultInstance()
				.constructMapType(HashMap.class, String.class, Object.class));

		@Override
		public final Map<String, Object>[] der(CharSequence from, Class<?>... tos) {
			return (Map<String, Object>[]) super.der(from, tos);
		}

		@Override
		protected <T> T single(JsonNode node, Class<T> c) {
			return (T) super.single(node, Map.class);
		}

		@Override
		public <T extends Map<String, Object>> T der(CharSequence from) {
			if (null == from) return null;
			try {
				return Jsons.mapper.readValue(from.toString(), t);
			} catch (IOException e) {
				return null;
			}
		}

		public <T extends Map<String, Object>> List<T> ders(CharSequence from) {
			Object r;
			if (null == from) return null;
			try {
				r = Jsons.mapper.readValue(from.toString(), t);
			} catch (IOException e) {
				try {
					r = Jsons.mapper.readValue(from.toString(), ts);
				} catch (IOException e1) {
					return null;
				}
			}
			if (r instanceof HashMap) {
				List<Map<String, Object>> list = new ArrayList<>();
				list.add((T) r);
				return (List<T>) list;
			} else if (r instanceof List) { return (List<T>) r; }
			throw new RuntimeException("Not support this type:" + r.getClass().getName());
		}

		public String sers(Map<String, Object>[] v) {
			try {
				return Jsons.mapper.writeValueAsString(v);
			} catch (JsonProcessingException e) {
				return null;
			}
		}

		@Override
		public <T extends Map<String, Object>> T der(CharSequence from, Class<T> to) {
			return ((ClassInfoSerder<Map<String, Object>, CharSequence>) this).der(from);
		}
	}
}
