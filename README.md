1、实现了hive通过jdbc读写mysql、pg关系型数据库。  
2、主要为了方便数据组从rdb到hive之间数据迁移  
3、使用Wiki：https://wiki.wdw.com/confluence/pages/viewpage.action?pageId=asdf  

部署方面hive2.3.6
1、hive已经包含了pg的jar，但是版本太低会报错，需要替换一下至少为9.4.1211
2、derby 版本需要升级