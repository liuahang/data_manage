package cn.chinacloud.mapper;

import java.util.List;

import cn.chinacloud.model.CatalogResource;
import cn.chinacloud.model.ResourceClassify;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.springframework.transaction.annotation.Transactional;

@Transactional
public interface CatalogResourceMapper {

	@Select("select id AS id,name AS name,original_name AS originalName,description AS description,open_property AS openProperty,open_condition AS openCondition, deleted AS deleted,created_at AS createdAt, updated_at AS updatedAt, structured AS structured from catalog_resources")
	public List<CatalogResource> findResouceInfoList();

	@Select("select id AS id,name AS name,original_name AS originalName,description AS description,open_property AS openProperty,open_condition AS openCondition, deleted AS deleted,created_at AS createdAt, updated_at AS updatedAt, structured AS structured from catalog_resources where id = (select catalog_id from catalog_classify_mapper where classify_id = #{id})")
	public List<CatalogResource> findResourceInfoByClassifyId(@Param("id") Integer id);
}
