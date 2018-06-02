--新客客户群[20171201-20180522]
SELECT
    deviceid
FROM
    src_huidu_zl.devaccum2
WHERE
    (khh IS NULL OR length(trim(khh)) = 0) 
    and
    from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    and  
    from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') <= '20180522'
    and hdfs_par = '20180521'

--浏览理财总人数
select
	count(1)
from
(
	--新客客户池选择：20171201-20180520
	SELECT
		deviceid
	FROM
		src_huidu_zl.devaccum2
	WHERE
		(khh IS NULL OR length(trim(khh)) = 0) 
		and
		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
		and 
		hdfs_par = '20180522'
) t1
join
(
	select
		deviceid,max(recvtime) as recvtime
	from
		src_huidu_zl.page2
	where
		hdfs_par >= '20171201' and hdfs_par <= '20180522' and page_id like '%licai%'
	group by deviceid 
) t2 on t1.deviceid = t2.deviceid
--浏览资讯总人数
select
	count(1)
from
(
	--新客客户池选择：20171201-20180520
	SELECT
		deviceid
	FROM
		src_huidu_zl.devaccum2
	WHERE
		(khh IS NULL OR length(trim(khh)) = 0) 
		and
		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
		and 
		hdfs_par = '20180522'
) t1
join
(
	select
		deviceid,max(recvtime) as recvtime
	from
		src_huidu_zl.page2
	where
		hdfs_par >= '20171201' and hdfs_par <= '20180522' and page_id like '%zixun%'
	group by deviceid 
) t2 on t1.deviceid = t2.deviceid


--浏览理财总次数[20171201-20180522]
select
    sum(t.cnt)
from
(
    select
    	t2.deviceid,t2.cnt
    from
    (
    	SELECT
    		deviceid
    	FROM
    		src_huidu_zl.devaccum2
    	WHERE
    		(khh IS NULL OR length(trim(khh)) = 0) 
    		and
    		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    		and 
    		hdfs_par = '20180522'
    ) t1
    join
    (
    	select 
    		deviceid,count(1) as cnt
    	from
    		src_huidu_zl.page2
    	where
    		hdfs_par >= '20171201'
    		and hdfs_par <= '20180522' 
    		and page_id like '%licai%'
    	group by deviceid
    ) t2 on t1.deviceid = t2.deviceid --order by t2.cnt desc
) t

--浏览资讯总次数[20171201-20180522]
select
    sum(t.cnt)
from
(
    select
    	t2.deviceid,t2.cnt
    from
    (
    	SELECT
    		deviceid
    	FROM
    		src_huidu_zl.devaccum2
    	WHERE
    		(khh IS NULL OR length(trim(khh)) = 0) 
    		and
    		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    		and 
    		hdfs_par = '20180522'
    ) t1
    join
    (
    	select 
    		deviceid,count(1) as cnt
    	from
    		src_huidu_zl.page2
    	where
    		hdfs_par >= '20171201'
    		and hdfs_par <= '20180522' 
    		and page_id like '%资讯%'
    	group by deviceid
    ) t2 on t1.deviceid = t2.deviceid --order by t2.cnt desc
) t

--新客选择为半年时间20171201-20180520
--RFM统计：R:最后一次操作距今时间间隔；F:最近某一时间间隔内的操作
--从下载日期（20171201）起，截止20180520，最近一次进行页面浏览资讯操作距20180520天数
select
    count(*)
from
(
    select
    	t2.deviceid,
    	t2.recvtime,
    	datediff(now() - interval 24 hours,from_unixtime(unix_timestamp(t2.recvtime),'yyyy-MM-dd')) AS interval_days
    from
    (
    	--新客客户池选择：20171201-20180520
    	SELECT
    	    deviceid
    	FROM
    		src_huidu_zl.devaccum2
    	WHERE
    		(khh IS NULL OR length(trim(khh)) = 0) 
    		and
    		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    		and 
    		hdfs_par = '20180522'
    ) t1
    join
    (
    	select
    		deviceid,max(recvtime) as recvtime
    	from
    		src_huidu_zl.page2
    	where
    		hdfs_par >= '20171201' and hdfs_par <= '20180522' and page_id like '%zixun%'
    	group by deviceid 
    ) t2 on t1.deviceid = t2.deviceid --order by install_day asc,interval_days asc
) tt where tt.interval_days <= 180


--hive
select
    count(*)
from
(
    select
    	t2.deviceid,
    	t2.recvtime,
    	datediff(date_add(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),-1),from_unixtime(unix_timestamp(recvtime),'yyyy-MM-dd')) AS interval_days
    from
    (
    	--新客客户池选择：20171201-20180520
    	SELECT
    	    deviceid
    	FROM
    		src_huidu_zl.devaccum2
    	WHERE
    		(khh IS NULL OR length(trim(khh)) = 0) 
    		and
    		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    		and 
    		hdfs_par = '20180522'
    ) t1
    join
    (
    	select
    		deviceid,max(recvtime) as recvtime
    	from
    		src_huidu_zl.page2
    	where
    		hdfs_par >= '20171201' and hdfs_par <= '20180522' and page_id like '%licai%'
    	group by deviceid 
    ) t2 on t1.deviceid = t2.deviceid --order by install_day asc,interval_days asc
) tt where tt.interval_days <= 15

--下载日期20171201-20180522，距今15/30/60/90/150/180天的使用频率
select
    sum(t.cnt)
from
(
    select
    	t2.deviceid,t2.cnt
    from
    (
    	SELECT
    		deviceid
    	FROM
    		src_huidu_zl.devaccum2
    	WHERE
    		(khh IS NULL OR length(trim(khh)) = 0) 
    		and
    		from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
    		and 
    		hdfs_par = '20180522'
    ) t1
    join
    (
    	select 
    		deviceid,count(1) as cnt
    	from
    		src_huidu_zl.page2
    	where
    		hdfs_par >= from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),-16),'yyyy-MM-dd'),'yyyyMMdd') 
    		and hdfs_par <= '20180522' 
    		and page_id like '%zixun%'
    	group by deviceid
    ) t2 on t1.deviceid = t2.deviceid --order by t2.cnt desc
) t

--下载日期20171201-20180522，距今15/30/60/90/150/180天的使用天数
select
	sum(t.days)
from
(
	select
		tt.deviceid,count(1) as days
	from
	(
		select
			t2.deviceid
		from
		(
			SELECT
				deviceid
			FROM
				src_huidu_zl.devaccum2
			WHERE
				(khh IS NULL OR length(trim(khh)) = 0) 
				and
				from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
				and 
				hdfs_par = '20180522'
		) t1
		join
		(
			--按天统计deviceID总量
			select 
				hdfs_par,deviceid,count(1) as cnt
			from
				src_huidu_zl.page2
			where
				hdfs_par >= from_unixtime(unix_timestamp(date_add(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),-16),'yyyy-MM-dd'),'yyyyMMdd') 
				and hdfs_par <= '20180522' 
				and page_id like '%zixun%'
			group by hdfs_par,deviceid
		) t2 on t1.deviceid = t2.deviceid
	) tt group by tt.deviceid
) t;


----------------------------------------------------------------------------rfm数据计算---------------------------------------------------------------------------------
--[20171201-20180526]位绑定客户号的用户，关联获取page_id/page_title/进入页面时间，并注册临时表ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
--建表语句
create table ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info(
    deviceid    string,
    app_install_time    string,
    page_id string,
    recvtime    string
)
comment '涨乐近期6个月浏览数据临时表'  
row format delimited 
fields terminated by '\t' 
lines terminated by '\n'  
stored as textfile;

--数据计算逻辑：半年内[20171201-20180530]下载安装了涨乐APP，截止20180530未绑定客户号的用户[150万]，并且获取这些用户的浏览数据（不区分page_id）[8.9亿]
insert overwrite table ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
SELECT
    t1.deviceid,
    t1.app_install_time,
    t2.page_id,
    t2.recvtime
FROM
(
    SELECT
        deviceid,
        app_install_time
    FROM
        src_huidu_zl.devaccum2
    WHERE
        (khh IS NULL OR length(trim(khh)) = 0) 
        and deviceid IS NOT NULL
        and length(trim(deviceid)) != 0
        --数据清洗，安装时间有可能超前
        and from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') >= '20171201'
        and from_unixtime(unix_timestamp(app_install_time),'yyyyMMdd') <= '20180530'
        and hdfs_par = '20180530'
) t1
join
(
    SELECT
        deviceid,
        page_id,
        time as recvtime
    FROM
        ana_fx_middle.zl_mid_daily_page
    where
        hdfs_par >= '20171201'
        and 
        hdfs_par <= '20180530'
        --数据清洗，存在大于当前的不合理数据
        and from_unixtime(unix_timestamp(time),'yyyyMMdd') >= '20171201'
        and from_unixtime(unix_timestamp(time),'yyyyMMdd') <= '20180530' 
) t2 on t1.deviceid = t2.deviceid;


--[20171201-20180523]rmf结果存储表ana_crmpicture.temp_lftuo_20180530_zhangle_6m_rfm
--r_*:6个月最后几次操作距今天数；f_*:6个月浏览总次数；m_*:6个月浏览天数
create table ana_crmpicture.temp_lftuo_20180530_zhangle_6m_rfm(
    deviceid    string, --设备号
    r_zixun string, --资讯R维度
    f_zixun string, --资讯F维度
    m_zixun string, --资讯M维度
    r_licai string, --理财R维度
    f_licai string, --理财F维度
    m_licai string, --理财M维度
    r_gegu  string, --个股R维度
    f_gegu  string, --个股F维度
    m_gegu  string, --个股M维度
    r_lemi  string, --乐米R维度
    f_lemi  string, --乐米F维度
    m_lemi  string  --乐米M维度
)
comment '涨乐近期6个月浏览数据临时表'  
row format delimited 
fields terminated by '\t' 
lines terminated by '\n'  
stored as textfile;

--资讯6个月R维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_zixun;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_zixun AS
SELECT
    t1.deviceid,
    datediff(from_unixtime(unix_timestamp('20180531','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t1.max_recvtime),'yyyy-MM-dd')) AS r_zixun
FROM
(
    SELECT
        deviceid,max(recvtime) as max_recvtime
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE 
        page_id like '%leaf63%'
    group by deviceid
) t1;

--资讯6个月F维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_zixun;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_zixun AS
SELECT
    t2.deviceid,t2.f_zixun
FROM
(
    SELECT 
        deviceid,count(1) as f_zixun
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf63%'
    group by deviceid
) t2;

--资讯6个月M维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_zixun;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_zixun AS
SELECT
    t3.deviceid,t3.m_zixun
FROM
(
    SELECT 
        deviceid,count(distinct(from_unixtime(unix_timestamp(recvtime),'yyyyMMdd'))) as m_zixun
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf63%'
    group by deviceid
) t3;

--理财6个月R维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_licai;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_licai AS
SELECT
    t4.deviceid,
    datediff(from_unixtime(unix_timestamp('20180531','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t4.max_recvtime),'yyyy-MM-dd')) AS r_licai
FROM
(
    SELECT
        deviceid,max(recvtime) as max_recvtime
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE 
        page_id like '%leaf360%'
    group by deviceid
) t4;

--理财6个月F维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_licai;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_licai AS
SELECT
    t5.deviceid,t5.f_licai
FROM
(
    SELECT 
        deviceid,count(1) as f_licai
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf360%'
    group by deviceid
) t5;

--理财6个月M维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_licai;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_licai AS
SELECT
    t6.deviceid,t6.m_licai
FROM
(
    SELECT 
        deviceid,count(distinct(from_unixtime(unix_timestamp(recvtime),'yyyyMMdd'))) as m_licai
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf360%'
    group by deviceid
) t6;

--个股6个月R维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_gegu;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_gegu AS
SELECT
    t7.deviceid,
    datediff(from_unixtime(unix_timestamp('20180531','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t7.max_recvtime),'yyyy-MM-dd')) AS r_gegu
FROM
(
    SELECT
        deviceid,max(recvtime) as max_recvtime
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE 
        page_id like '%leaf247%'
    group by deviceid
) t7;

--个股6个月F维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_gegu;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_gegu AS
SELECT
    t8.deviceid,t8.f_gegu
FROM
(
    SELECT 
        deviceid,count(1) as f_gegu
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf247%'
    group by deviceid
) t8;

--个股6个月M维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_gegu;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_gegu AS
SELECT
    t9.deviceid,t9.m_gegu
FROM
(
    SELECT 
        deviceid,count(distinct(from_unixtime(unix_timestamp(recvtime),'yyyyMMdd'))) as m_gegu
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf247%'
    group by deviceid
) t9;


--乐米6个月R维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_lemi;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_lemi AS
SELECT
    t10.deviceid,
    datediff(from_unixtime(unix_timestamp('20180531','yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t10.max_recvtime),'yyyy-MM-dd')) AS r_lemi
FROM
(
    SELECT
        deviceid,max(recvtime) as max_recvtime
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE 
        page_id like '%leaf59%'
    group by deviceid
) t10;

--乐米6个月F维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_lemi;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_lemi AS
SELECT
    t11.deviceid,t11.f_lemi
FROM
(
    SELECT 
        deviceid,count(1) as f_lemi
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf59%'
    group by deviceid
) t11;

--乐米6个月M维度统计
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_lemi;
create table if not exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_lemi AS
SELECT
    t12.deviceid,t12.m_lemi
FROM
(
    SELECT 
        deviceid,count(distinct(from_unixtime(unix_timestamp(recvtime),'yyyyMMdd'))) as m_lemi
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
    WHERE
        page_id like '%leaf59%'
    group by deviceid
) t12;

--结果表插入：r指标异常值填充500，f/m指标异常值填充0；关联条件：r/m指标<=181(半年的天数)
insert overwrite table ana_crmpicture.temp_lftuo_20180530_zhangle_6m_rfm
SELECT
    tt0.deviceid,
    nvl(tt1.r_zixun,500),   --资讯r指标：无浏览数据，则填充异常值500
    nvl(tt2.f_zixun,0), --资讯f指标：无浏览数据，则填充异常值0
    nvl(tt3.m_zixun,0), --资讯m指标：无浏览数据，则填充异常值0
    nvl(tt4.r_licai,500),   --理财r指标：无浏览数据，则填充异常值500
    nvl(tt5.f_licai,0), --理财f指标：无浏览数据，则填充异常值0
    nvl(tt6.m_licai,0), --理财m指标：无浏览数据，则填充异常值0
    nvl(tt7.r_gegu,500),    --个股r指标：无浏览数据，则填充异常值500
    nvl(tt8.f_gegu,0),  --个股f指标：无浏览数据，则填充异常值0
    nvl(tt9.m_gegu,0),  --个股m指标：无浏览数据，则填充异常值0
    nvl(tt10.r_lemi,500),   --乐米r指标：无浏览数据，则填充异常值500
    nvl(tt11.f_lemi,0), --乐米f指标：无浏览数据，则填充异常值0
    nvl(tt12.m_lemi,0)  --乐米m指标：无浏览数据，则填充异常值0
FROM
(
    SELECT
        distinct deviceid
    FROM
        ana_crmpicture.temp_lftuo_20180530_zhangle_6m_page_info
) tt0
left join 
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_zixun tt1 on tt0.deviceid = tt1.deviceid and tt1.r_zixun <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_zixun tt2 on tt0.deviceid = tt2.deviceid
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_zixun tt3 on tt0.deviceid = tt3.deviceid and tt3.m_zixun <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_licai tt4 on tt0.deviceid = tt4.deviceid and tt4.r_licai <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_licai tt5 on tt0.deviceid = tt5.deviceid
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_licai tt6 on tt0.deviceid = tt6.deviceid and tt6.m_licai <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_gegu  tt7 on tt0.deviceid = tt7.deviceid and tt7.r_gegu <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_gegu  tt8 on tt0.deviceid = tt8.deviceid
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_gegu  tt9 on tt0.deviceid = tt9.deviceid and tt9.m_gegu <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_lemi  tt10 on tt0.deviceid = tt10.deviceid and tt10.r_lemi <= 181
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_lemi  tt11 on tt0.deviceid = tt11.deviceid
left join  
ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_lemi  tt12 on tt0.deviceid = tt12.deviceid and tt12.m_lemi <= 181;
--删除临时表
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_zixun;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_zixun;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_zixun;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_licai;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_licai;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_licai;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_gegu;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_gegu;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_gegu;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_r_lemi;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_f_lemi;
drop table if exists ana_crmpicture.temp_lftuo_20180530_zhangle_6m_m_lemi;
--------------------------------------------------------------------------rfm数据计算end--------------------------------------------------------------------------------

--数据异常检查SQL
SELECT
    *
FROM
    ana_crmpicture.temp_lftuo_20180530_zhangle_6m_rfm
WHERE
    r_zixun < '0' or
    f_zixun < '0' or
    m_zixun < '0' or
    r_licai < '0' or
    f_licai < '0' or
    m_licai < '0' or
    r_gegu < '0' or
    f_gegu < '0' or
    m_gegu < '0' or
    r_lemi < '0' or
    f_lemi < '0' or
    m_lemi  < '0';
