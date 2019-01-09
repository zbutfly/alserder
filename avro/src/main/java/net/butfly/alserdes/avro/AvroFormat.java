package net.butfly.alserdes.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import net.butfly.albatis.ddl.FieldDesc;
import net.butfly.albatis.io.Rmap;
import net.butfly.alserdes.SerDes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static net.butfly.albatis.ddl.vals.ValType.Flags.*;

@SerDes.As("kafka")
public class AvroFormat implements Serializable {
    private static final long serialVersionUID = 3687957634350865452L;

    public byte[] ser(Rmap src, FieldDesc... fields) {
        byte[] avroRecordBytes;
        Schema schema = getSchema(fields);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        GenericData.Record avroRecord = new GenericData.Record(schema);
        src.map().forEach(avroRecord::put);
        avroRecordBytes = recordInjection.apply(avroRecord);
        return avroRecordBytes;
    }

    public GenericRecord deser(byte[] bytes, FieldDesc... fields) {
        Schema schema = getSchema(fields);
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
        return recordInjection.invert(bytes).get();
    }

    private Schema getSchema(FieldDesc... fieldDescs) {
        List<Schema.Field> fieldList = new ArrayList<>();
        for (FieldDesc f : fieldDescs) {
            fieldList.add(getSchemaField(f));
        }
        //创建匿名的record schema
        return Schema.createRecord(fieldList);
    }

    private Schema.Field getSchemaField(FieldDesc f) {
        if (null == f) return null;
        switch (f.type.flag) {
            case BINARY:
                return new Schema.Field(f.name, Schema.create(Schema.Type.BYTES), null, new byte[0]);
            case INT:
                return new Schema.Field(f.name, Schema.create(Schema.Type.INT), null, 0);
                //TODO Data类型的处理
            case DATE:
            case LONG:
                return new Schema.Field(f.name, Schema.create(Schema.Type.LONG), null, 0);
            case FLOAT:
                return new Schema.Field(f.name, Schema.create(Schema.Type.FLOAT), null, 0.0);
            case DOUBLE:
                return new Schema.Field(f.name, Schema.create(Schema.Type.DOUBLE), null, 0.0);
            case BOOL:
                return new Schema.Field(f.name, Schema.create(Schema.Type.BOOLEAN), null, false);
            case CHAR:
            case STR:
            case STRL:
            case GEO:
            case JSON_STR:
            case UNKNOWN:
                return new Schema.Field(f.name, Schema.create(Schema.Type.STRING), null, "");
        }
        return null;
    }


}
