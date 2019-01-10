package net.butfly.alserdes.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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
		return null == s ? null : JsonSerder.JSON_MAPPER.der(s);
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
			return Jsons.mapper.readValue(s, Jsons.mapper.getTypeFactory().constructCollectionType(List.class, Map.class));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public Class<?> formatClass() {
		return String.class;
	}
}
