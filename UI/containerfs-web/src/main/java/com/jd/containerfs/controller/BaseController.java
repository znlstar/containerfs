package com.jd.containerfs.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.util.NestedServletException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Created by lixiaoping3 on 17-11-23.
 * 异常处理
 */
public abstract class BaseController {

    @ExceptionHandler(value = {Exception.class})
    public void actionExceptionHandler(Throwable ex, HttpServletRequest request, HttpServletResponse response)
            throws IOException, NestedServletException {

        JSONObject jsonObject = null;
        String requestType = request.getHeader("X-Requested-With");
        boolean isAjax = "XMLHttpRequest".equals(requestType);
        if(!isAjax){
            String message=ex.getMessage();
            redirectErrorPage(message,request,response);
        }else{
            System.out.println("ajax request failed");
        }
    }

    private void redirectErrorPage(String message, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        request.setAttribute("message", message);
        RequestDispatcher view = request.getRequestDispatcher("/error.jsp");
        try {
            view.forward(request, response);
        } catch (ServletException e) {
            e.printStackTrace();
        }
    }

    protected void responseWrite(HttpServletResponse response, JSONObject jsonObject) throws IOException {
        response.setContentType("text/html; charset=utf-8");
        response.setCharacterEncoding("utf-8");
    }
}
