package com.jd.containerfs.common.util;

import org.dozer.DozerBeanMapper;

/**
 * Created by lixiaoping3 on 17-11-20.
 */
public class DozerMapper {

    public static final DozerBeanMapper dozer=new DozerBeanMapper();
    /**
     * 构造新的destinationClass实例对象，通过source对象中的字段内容
     * 映射到destinationClass实例对象中，并返回新的destinationClass实例对象。
     *
     * @param source 源数据对象
     * @param destinationClass 要构造新的实例对象Class
     */
    public static <T> T map(Object source, Class<T> destinationClass)
    {
        return dozer.map(source, destinationClass);
    }
}
