package cn.chinacloud.service;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.chinacloud.mapper.CatalogResourceMapper;
import cn.chinacloud.model.CatalogResource;


@Service
public class ResourceService {

    @Autowired
    private CatalogResourceMapper catalogResourceMapper;

    public List<CatalogResource> getResourceInfo(){
    	List<CatalogResource> resource=catalogResourceMapper.findResouceInfoList();
        return resource;
    }

}
