package com.xiaoxiaomo.flink.demo;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import java.io.IOException;

public class UserSchema implements DeserializationSchema<User>, SerializationSchema<User> {

    private static final Gson gson = new Gson();

    /**
     * 反序列化，将byte数组转成Student实例
     * @param bytes
     * @return
     * @throws IOException
     */
    @Override
    public User deserialize(byte[] bytes) throws IOException {
        return gson.fromJson(new String(bytes), User.class);
    }

    @Override
    public boolean isEndOfStream(User user) {
        return false;
    }

    /**
     * 序列化，将Student实例转成byte数组
     * @param user
     * @return
     */
    @Override
    public byte[] serialize(User user) {
        return new byte[0];
    }

    @Override
    public TypeInformation<User> getProducedType() {
        return TypeInformation.of(User.class);
    }
}

