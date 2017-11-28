package com.jd.containerfs.common.util;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class BeanUtils {

    public static final <K, V> Map<K, V> toMap(Object obj) throws Exception {
        if (obj == null) {
            return null;
        }
        return JSONHelper.fromJson(JSONHelper.toJSONAsBytes(obj), Map.class);
    }

    public static final <O, D> D copy(O ori, Class<D> descClass) throws Exception {
        if (ori == null) {
            return null;
        }
        return JSONHelper.fromJson(JSONHelper.toJSONAsBytes(ori), descClass);
    }

    public static final <O, D> List<D> copy(List<O> oriList, Class<D> descClass) throws Exception {
        if (oriList == null) {
            return null;
        }
        List<D> descList = Lists.newArrayList();
        oriList.forEach((ori) -> {
            try {
                descList.add(JSONHelper.fromJson(JSONHelper.toJSONAsBytes(ori), descClass));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return descList;
    }

}
