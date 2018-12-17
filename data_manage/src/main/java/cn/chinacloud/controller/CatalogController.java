package cn.chinacloud.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import cn.chinacloud.model.CatalogResource;
import cn.chinacloud.service.ResourceService;


@Controller
public class CatalogController {

    private Logger logger = Logger.getLogger(CatalogController.class);

    @Autowired
    private ResourceService resourceService;

    @RequestMapping("/getResourceInfo")
    @ResponseBody
    public List<CatalogResource> getResourceInfo() {
    	List<CatalogResource> resouceList = resourceService.getResourceInfo();
        if(resouceList!=null){
        	CatalogResource resouce =  resouceList.get(0);
            System.out.println("resouce.getName():"+resouce.getName());
            logger.info("resouce.getName():"+resouce.getName());
        }
        return resouceList;
    }
}
