package service.serviceImpl;

import dao.FlowStatDao;
import dao.daoImpl.FlowStatDaoImp;
import domain.FlowStat;
import service.FlowStatService;

import java.util.ArrayList;

/**
 * Created by Administrator on 2019/1/8.
 */
public class FlowStatServiceImp implements FlowStatService {

    FlowStatDao flowStatDao = new FlowStatDaoImp();

    @Override
    public ArrayList<FlowStat> getUV() {
       return  flowStatDao.getUv();
    }
}
