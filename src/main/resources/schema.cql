DROP TABLE IF EXISTS guidelinerule;
CREATE TABLE guidelinerule (
  EXAMGUIDELINE_ID varchar PRIMARY KEY,//guide line id. it will be composited by <guidelineid>_<risktype> 
  NAME varchar,
  ORIGINATE varchar,//guide line source
  DESCRIPTION text,
  CONCERNEDFACTORS text,//the text for which factors are concerned
  RISKTYPE varchar,//to define risk type: low,high
  RISKDEFINE text,//the text for risk description
  RISKEXPRESSION text,//the expression to calculate if the guide line can be applied
  STATUS varchar,
  CREATEON timestamp,
  TARGETDISEASE varchar,//which disease can be detected. optional
);

DROP TABLE IF EXISTS user;
CREATE TABLE user (
  USER_ID varchar PRIMARY KEY,
  USERNAME varchar,
  PASSWORD varchar,
  NAME varchar,
  EMAIL varchar,
  PHONE varchar,
  OPENID varchar,
  ALIAS varchar,
  BIRTHDAY date,
  SEX varchar,
  BIRTHPLACE varchar,
  LIVEPLACE varchar,
  MARRIAGESTATUS varchar,
  CAREER varchar,
  DEGREE varchar,
  AVATAR varchar,
  HEIGHT int,
  WEIGHT int,
  diseases_history set<varchar>,
  diseases_concerned set<varchar>,
  diseases_inhertied set<varchar>,
  tags map<varchar,varchar>,//JSON:lifestyle tags. This is NOT a single column. Detailed columns will be created one by one
  lastModifiedOn timestamp, //timestamp of user profile last modified
  lastEvaluatedOn timestamp, //timestamp of user checkup solution evaluated
  summaryAvatar varchar //profile image with tags 
);

DROP TABLE IF EXISTS checkuppackage;
CREATE TABLE checkuppackage(
  CHECKUPPACKAGE_ID varchar primary key,
  worker varchar,
  generateTime timestamp,
  effectiveTime timestamp,
  expireOn timestamp,
  status varchar,
  revision int,
  sysflag varchar,
  USER_ID varchar,
  description text //auto generated solution description text
);

DROP TABLE IF EXISTS checkupitem;
CREATE TABLE checkupitem (
  CHECKUPITEM_ID VARCHAR primary key,
  SUBGROUP VARCHAR,
  NAME VARCHAR,
  FEATURES set<varchar>,
  FREQUENCY varchar,
  STATUS varchar,
  ORIGINATE varchar,
  DESCRIPTION text,
  GENERATEDTIME timestamp,
  WORKER VARCHAR,
  REVISION INT,
  SYSFLAG VARCHAR,
  CHECKUPPACKAGE_ID varchar,
  USER_ID VARCHAR
);