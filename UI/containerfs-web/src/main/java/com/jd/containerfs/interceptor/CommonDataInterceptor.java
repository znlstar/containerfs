package com.jd.containerfs.interceptor;

import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;

/**
 * 通用数据拦截器
 */
public class CommonDataInterceptor extends HandlerInterceptorAdapter {


    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        StringBuffer requestUrl = request.getRequestURL();
        requestUrl.append("?fromSite=containerfs");

        Boolean isPageUrl = false;

        //获取请求参数
        Enumeration enu = request.getParameterNames();
        while (enu.hasMoreElements()) {
            String paraName = (String) enu.nextElement();
            if (paraName.equals("pn") || paraName.equals("pageSize")) {
                isPageUrl = true;
            }

            if (isPageUrl && !paraName.equals("pn")) {
                requestUrl.append("&" + paraName + "=" + request.getParameter(paraName));
            }
        }

        if (modelAndView != null) {
            modelAndView.addObject("requestUrl", requestUrl);
        }
    }

}
