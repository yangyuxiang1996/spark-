```
== Physical Plan ==
*(5) SortMergeJoin [md5_imei#16], [device_id#10], Inner
:- *(2) Sort [md5_imei#16 ASC NULLS FIRST], false, 0
:  +- Exchange hashpartitioning(md5_imei#16, 600)
:     +- *(1) Project [md5_imei#16, raw_idfa#17, uuid_type#32, timestamp#38L, log_type#82, mz_spot_id#34L, mz_campaign_id#33, platform#52]
:        +- *(1) Filter ((((isnotnull(platform#52) && ((uuid_type#32 = md5_imei) || (uuid_type#32 = raw_idfa))) && (NOT (length(md5_imei#16) = 0) || NOT (length(raw_idfa#17) = 0))) && (cast(platform#52 as int) = 1)) && isnotnull(md5_imei#16))
:           +- *(1) FileScan orc [md5_imei#16,raw_idfa#17,uuid_type#32,mz_campaign_id#33,mz_spot_id#34L,timestamp#38L,platform#52,log_type#82] Batched: true, Format: ORC, Location: InMemoryFileIndex[hdfs://adhnamenode/user/mz_supertool/datalake/src/admonitor/ods_adm_bus/20200831], PartitionFilters: [], PushedFilters: [IsNotNull(platform), Or(EqualTo(uuid_type,md5_imei),EqualTo(uuid_type,raw_idfa)), IsNotNull(md5_..., ReadSchema: struct<md5_imei:string,raw_idfa:string,uuid_type:string,mz_campaign_id:int,mz_spot_id:bigint,time...
+- *(4) Sort [device_id#10 ASC NULLS FIRST], false, 0
   +- *(4) HashAggregate(keys=[device_id#10], functions=[])
      +- Exchange hashpartitioning(device_id#10, 600)
         +- *(3) HashAggregate(keys=[device_id#10], functions=[])
            +- *(3) Project [value#8 AS device_id#10]
               +- *(3) Filter isnotnull(value#8)
                  +- *(3) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#8]
                     +- *(3) MapElements <function1>, obj#7: java.lang.String
                        +- *(3) DeserializeToObject value#0.toString, obj#6: java.lang.String
                           +- *(3) FileScan text [value#0] Batched: false, Format: Text, Location: InMemoryFileIndex[hdfs://adhnamenode/user/mz_supertool/workspace/yyx/weiboID/output/joined_statis..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<value:string>
```

* Exchange hashpartitioning

  

* SortMergeJoin
* HashAggregate
* SerializeFromObject
* DeserializeToObject
* MapElements

```
*(1) Filter <function1>.apply
+- *(1) Project [md5_imei#17, raw_idfa#18, uuid_type#33, timestamp#39L, log_type#83, mz_spot_id#35L, mz_campaign_id#34, platform#53]
   +- *(1) Filter (((isnotnull(platform#53) && ((uuid_type#33 = md5_imei) || (uuid_type#33 = raw_idfa))) && (NOT (length(md5_imei#17) = 0) || NOT (length(raw_idfa#18) = 0))) && (cast(platform#53 as int) = 1))
      +- *(1) FileScan orc [md5_imei#17,raw_idfa#18,uuid_type#33,mz_campaign_id#34,mz_spot_id#35L,timestamp#39L,platform#53,log_type#83] Batched: true, Format: ORC, Location: InMemoryFileIndex[hdfs://adhnamenode/user/mz_supertool/datalake/src/admonitor/ods_adm_bus/20200831], PartitionFilters: [], PushedFilters: [IsNotNull(platform), Or(EqualTo(uuid_type,md5_imei),EqualTo(uuid_type,raw_idfa))], ReadSchema: struct<md5_imei:string,raw_idfa:string,uuid_type:string,mz_campaign_id:int,mz_spot_id:bigint,time...
```

