package net.butfly.alserder.json;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;

import net.butfly.albacore.serder.JsonSerder;
import net.butfly.albacore.serder.json.Jsons;
import net.butfly.alserder.SerDes;
import net.butfly.alserder.SerDes.SerAs;

@SerAs(format = "json", from = Map.class, to = String.class)
public class JsonSerDes implements SerDes<Map<String, Object>, String> {
	private static final long serialVersionUID = 6767740047890492594L;

	@Override
	public String ser(Map<String, Object> m) {
		return null == m ? null : JsonSerder.JSON_MAPPER.ser(m);
	}

	@Override
	public Map<String, Object> deser(String s) {
		return null == s ? null : JsonSerder.JSON_MAPPER.der(s);
	}

	@SuppressWarnings("unchecked")
	@Override
	public String sers(Map<String, Object>... vs) {
		try {
			return Jsons.mapper.writeValueAsString(vs);
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public List<Map<String, Object>> desers(String s) {
		try {
			return Jsons.mapper.readValue(s, Jsons.mapper.getTypeFactory().constructCollectionType(List.class, String.class));
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
