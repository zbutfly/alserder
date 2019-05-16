package net.butfly.alserdes.json;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.serder.json.Jsons;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.SerDes.MapSerDes;

@SerDes.As("json")
@SerDes.As(value = "jsons", list = true)
public class JsonSerDes implements MapSerDes<String> {
	private static final long serialVersionUID = 6767740047890492594L;

	@Override
	public String ser(Map<String, Object> m) {
		return null == m ? null : JsonSerder.JSON_MAPPER.ser(m);
	}

	@Override
	public Map<String, Object> deser(String s) {
		Map<String, Object> map = null == s ? null : JsonSerder.JSON_MAPPER.der(s);
		if (null != map) removeNull(map);
		return map;
	}

	@Override
	public String sers(List<Map<String, Object>> l) {
		try {
			return Jsons.mapper.writeValueAsString(l);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public List<Map<String, Object>> desers(String s) {
		try {
			List<Map<String, Object>> list = Jsons.mapper.readValue(s, Jsons.mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
			if (null != list && !list.isEmpty()) for (Map<String, Object> map : list) removeNull(map);
			return list;
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public Class<?> formatClass() {
		return String.class;
	}

	private void removeNull(Map<String, Object> map) {
		Iterator<Entry<String, Object>> entries = map.entrySet().iterator();
		while (entries.hasNext()) {
			Entry<String, Object> e = entries.next();
			if (null == e.getKey() || null == e.getValue())
				entries.remove();
		}
	}
}
