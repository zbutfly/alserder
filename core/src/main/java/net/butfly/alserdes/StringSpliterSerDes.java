package net.butfly.alserdes;

import java.util.Map;

import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.alserdes.SerDes;
import net.butfly.alserdes.SerDes.MapSerDes;

@SerDes.As("strSpliter")
public class StringSpliterSerDes implements MapSerDes<String>  {

	private static final long serialVersionUID = 1L;
	Logger logger = Logger.getLogger(StringSpliterSerDes.class);
	
	@Override
	public Map<String, Object> deser(String r) {
		String splitFlag = Configs.get("format.split.flag", "#");
		String[] arr = r.split(splitFlag);
		Map<String, Object> m = Maps.of();
		for (int i = 0	; i < arr.length; i++) {
			m.put("a"+i, arr[i]);
		}
		return m;
	}
}
