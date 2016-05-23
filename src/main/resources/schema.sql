DROP TABLE IF EXISTS `ta_user`;
CREATE TABLE `ta_user` (
  `USER_ID` varchar(100) NOT NULL COMMENT '用户id',
  `USERNAME` varchar(255) DEFAULT NULL COMMENT '用户名',
  `NAME` varchar(255) DEFAULT NULL COMMENT '姓名',
  `IP` varchar(15) DEFAULT NULL COMMENT '用户登录ip地址',
  `STATUS` varchar(32) DEFAULT NULL COMMENT '状态',
  `EMAIL` varchar(32) DEFAULT NULL COMMENT '电子邮件',
  `PHONE` varchar(32) DEFAULT NULL COMMENT '电话',
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
  `HEIGHT` int(11) DEFAULT '170' COMMENT '身高',
  `WEIGHT` int(11) DEFAULT '65' COMMENT '体重',
  `age` int(11) DEFAULT '0',
  `sufferedDiseases` varchar(1024) DEFAULT '',
  `inheritDiseases` varchar(1024) DEFAULT '',
  `concernDiseases` varchar(1024) DEFAULT '',  
  `tags` varchar(1024) DEFAULT '',
  `lastModifiedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `lastPreparedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `lastMatchedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `lastGeneratedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `lastEvaluatedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',  
  PRIMARY KEY (`USER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `ta_userrule`;
CREATE TABLE `ta_userrule` (
  `rule_id` varchar(100) NOT NULL,
  `user_id` varchar(100) NOT NULL,
  `guideline_id` varchar(100) NOT NULL,
  `originate` varchar(100) DEFAULT '',
  `description` varchar(512) DEFAULT '',
  `concernedfactors` varchar(512) DEFAULT '',
  `riskDefine` varchar(512) DEFAULT '',
  `disease_name` varchar(100) DEFAULT '',
  `riskType` varchar(20) DEFAULT 'low',
  `ruleExpression` varchar(1024) DEFAULT '1=1',
  `status` varchar(20) DEFAULT 'pending',
  `createdOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `modifiedOn` timestamp NOT NULL DEFAULT '1980-01-01 00:00:00',
  `worker` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`rule_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;