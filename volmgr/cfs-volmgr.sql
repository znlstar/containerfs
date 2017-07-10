-- MySQL dump 10.16  Distrib 10.1.20-MariaDB, for Linux (x86_64)
--
-- Host: localhost    Database: localhost
-- ------------------------------------------------------
-- Server version	10.1.20-MariaDB

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `blk`
--

DROP TABLE IF EXISTS `blk`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `blk` (
  `blkid` bigint(32) NOT NULL AUTO_INCREMENT,
  `hostip` varchar(32) NOT NULL,
  `hostport` int(16) NOT NULL,
  `allocated` tinyint(2) DEFAULT NULL,
  `disabled` tinyint(2) DEFAULT NULL,
  `createdTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`blkid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `blkgrp`
--

DROP TABLE IF EXISTS `blkgrp`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `blkgrp` (
  `blkgrpid` bigint(32) NOT NULL AUTO_INCREMENT,
  `blks` varchar(32) NOT NULL,
  `volume_uuid` varchar(32) NOT NULL,
  `createdTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`blkgrpid`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `disks`
--

DROP TABLE IF EXISTS `disks`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `disks` (
  `ip` varchar(32) NOT NULL,
  `port` int(16) NOT NULL,
  `mount` varchar(32) NOT NULL,
  `total` bigint(32) NOT NULL,
  `used` bigint(32) DEFAULT NULL,
  `free` bigint(32) DEFAULT NULL,
  `statu` tinyint(2) DEFAULT NULL,
  `createdTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ip`,`port`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `repair`
--

DROP TABLE IF EXISTS `repair`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `repair` (
  `volid` varchar(32) NOT NULL,
  `blkgrpid` bigint(32) NOT NULL,
  `blkid` bigint(32) NOT NULL,
  `blkip` varchar(32) NOT NULL,
  `blkport` int(16) NOT NULL,
  `chkid` bigint(32) NOT NULL,
  `position` tinyint(8) NOT NULL,
  `status` tinyint(8) NOT NULL,
  `path` varchar(256) NOT NULL,
  `createdTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `volumes`
--

DROP TABLE IF EXISTS `volumes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `volumes` (
  `raftgroupid` bigint(32) NOT NULL AUTO_INCREMENT,
  `uuid` varchar(32) NOT NULL,
  `name` varchar(32) NOT NULL,
  `size` bigint(32) NOT NULL,
  `metadomain` varchar(32) NOT NULL,
  `createdTime` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`raftgroupid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

