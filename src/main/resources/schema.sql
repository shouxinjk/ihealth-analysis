DROP TABLE IF EXISTS `ta_user`;
CREATE TABLE `ta_user` (
  `USER_ID` varchar(100) NOT NULL COMMENT '用户id',
  `USERNAME` varchar(255) DEFAULT NULL COMMENT '用户名',
  `PASSWORD` varchar(255) DEFAULT NULL COMMENT '密码',
  `NAME` varchar(255) DEFAULT NULL COMMENT '姓名',
  `RIGHTS` varchar(255) DEFAULT NULL COMMENT '权限',
  `ROLE_ID` varchar(100) DEFAULT NULL COMMENT '角色id',
  `LAST_LOGIN` varchar(255) DEFAULT NULL COMMENT '最后登录时间',
  `IP` varchar(15) DEFAULT NULL COMMENT '用户登录ip地址',
  `STATUS` varchar(32) DEFAULT NULL COMMENT '状态',
  `BZ` varchar(255) DEFAULT NULL,
  `SKIN` varchar(100) DEFAULT NULL COMMENT '皮肤',
  `EMAIL` varchar(32) DEFAULT NULL COMMENT '电子邮件',
  `NUMBER` varchar(100) DEFAULT NULL,
  `PHONE` varchar(32) DEFAULT NULL COMMENT '电话',
  `SFID` varchar(100) DEFAULT NULL,
  `START_TIME` varchar(100) DEFAULT NULL,
  `END_TIME` varchar(100) DEFAULT NULL,
  `YEARS` int(10) DEFAULT NULL, 
  `CREATEBY` varchar(100) DEFAULT NULL,
  `CREATEON` varchar(100) DEFAULT NULL,  
  `OPENID` varchar(255) DEFAULT NULL COMMENT '对应微信OPENID',
  `ALIAS` varchar(255) DEFAULT NULL COMMENT '昵称',
  `BIRTHDAY` varchar(255) DEFAULT NULL COMMENT '生日',
  `SEX` varchar(255) DEFAULT NULL COMMENT '性别',
  `BIRTHPLACE` varchar(255) DEFAULT NULL COMMENT '出生地',
  `LIVEPLACE` varchar(255) DEFAULT NULL COMMENT '居住地',
  `MARRIAGESTATUS` varchar(255) DEFAULT NULL COMMENT '婚姻状态',
  `CAREER` varchar(255) DEFAULT NULL COMMENT '职业',
  `DEGREE` varchar(255) DEFAULT NULL COMMENT '学历',
  `AVATAR` varchar(255) DEFAULT NULL COMMENT '用户图像',
  `HEIGHT` int DEFAULT 170 COMMENT '身高',
  `WEIGHT` int DEFAULT 65 COMMENT '体重',
  diseases varchar(1024) default '',
  diseaseHighConcern varchar(1024) default '',
  diseaseInheritable varchar(1024) default '',
  tags varchar(1024) default '',
  tag_smoke boolean default false,
  tag_smoke_age int default 10,
  tag_smoke_amount int default 20,
  PRIMARY KEY (`USER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ta_userRule`;
CREATE TABLE `ta_userRule` (
  `rule_id` varchar(100) NOT NULL,
  `user_id` varchar(100) NOT NULL,
  `guideline_id` varchar(100) NOT NULL,
  originate varchar(100) default '',
  description varchar(512) default '',
  concernedfactors varchar(512) default '',
  riskDefine varchar(512) default '',
  disease_name varchar(100) default '',
  `riskType` varchar(20) DEFAULT 'low',
  `ruleExpression` varchar(1024) DEFAULT '1=1',
  `status` varchar(20) DEFAULT 'pending',
  `createdOn` timestamp default now(),
  `modifiedOn` timestamp default now(),
  `worker` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;