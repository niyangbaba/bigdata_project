package controller;

import com.alibaba.fastjson.JSON;
import domain.FlowStat;
import service.FlowStatService;
import service.serviceImpl.FlowStatServiceImp;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Administrator on 2019/1/8.
 */
@WebServlet("/flow/getUV")
public class FlowStatController extends HttpServlet {

    FlowStatService flowStatService = new FlowStatServiceImp();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        ArrayList<FlowStat> flowStats = flowStatService.getUV();

        String json = JSON.toJSONString(flowStats);

        response.getWriter().write(json);

    }


    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        this.doGet(req,resp);
    }
}
