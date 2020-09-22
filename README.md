# README

* 基础依赖  
Linux或Mac系统 && Rails 6 (空Rails项目)  
关系型数据库，当前只适配了postgres，其它数据库的适配改动很小  
字段命名按约定来，比如 member(s) 表 id 主键的关联外键名统一为 member_id  

* 合并方式  
keep_all策略，合并主表以及关联表，比对两个数据库中重复的ID，同步更新主表ID以及关联表外键；  
keep_assos策略，合并关联表和不重复的主表数据，因为主表重复的数据在largerDB已存在；  
删除策略，清除不需要同步的数据表；  

* 适用场景  
比如：复制正式数据库到测试数据库，支持主键外键为整型或UUID；  
比对两个数据库(相似的表结构)的重复数据；  
更新重复的主键和外键，然后执行数据无冲突合并；  

* 怎么使用  
Step1：配置database.yml两个数据库的连接  
Step2：在db_merger.rake修改要排除的数据表 `@except_tables = []`  
Step3：检查重复的数据 `rake db_merger:check_tables`  
Step4：确认要修改数据 `rake db_merger:check_tables apply_fix=1`  
Step5：建议使用Navicat将smallerDB表的数据全导出，再全导入到largerDB  
