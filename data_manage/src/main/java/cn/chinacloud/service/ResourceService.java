package cn.chinacloud.service;


import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.chinacloud.mapper.CatalogResourceMapper;
import cn.chinacloud.model.CatalogResource;

/**
 * Created by Administrator on 2018/12/17.
 */

@Service
public class ResourceService {

    @Autowired
    private CatalogResourceMapper catalogResourceMapper;

    /**
     *
     * @return List<CatalogResource>
     */
    public List<CatalogResource> getResourceInfo(){
    	List<CatalogResource> resource=catalogResourceMapper.findResouceInfoList();
        return resource;
    }

    /**
     *
     * @param id
     * @return List<CatalogResource>
     */

    public List<CatalogResource> getResourceInfoByClassifyId(Integer id){
        List<CatalogResource> resource=catalogResourceMapper.findResourceInfoByClassifyId(id);
        return resource;
    }
}
