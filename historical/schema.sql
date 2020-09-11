-- MySQL dump 10.13  Distrib 5.7.17, for Win64 (x86_64)
--
-- Host: 192.168.99.100    Database: qfx
-- ------------------------------------------------------
-- Server version	5.7.22

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
-- Table structure for table `trades_period`
--

DROP TABLE IF EXISTS `trades_period`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `trades_period` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `schedule_id` int(11) NOT NULL COMMENT 'ID of corresponding task',
  `starttime` bigint(20) NOT NULL COMMENT 'timestamp in milliseconds',
  `endtime` bigint(20) NOT NULL COMMENT 'timestamp in milliseconds',
  `cutoff` int(11) NOT NULL COMMENT 'maximum waiting period for a script before it cuts off',
  `status` set('PENDING','WORKING','FINISHED') NOT NULL DEFAULT 'PENDING',
  `container_name` varchar(255) DEFAULT NULL,
  `started_at` timestamp NULL DEFAULT NULL,
  `finished_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=49 DEFAULT CHARSET=latin1 COMMENT='smaller periods to be scraped by individual agents';
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `trades_schedule`
--

DROP TABLE IF EXISTS `trades_schedule`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `trades_schedule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `exchange` varchar(255) NOT NULL COMMENT 'e.g. BINANCE',
  `module` varchar(255) NOT NULL COMMENT 'BitfinexTradesREST etc',
  `market` text NOT NULL COMMENT 'list of all markets to be parsed, delimited by "_"',
  `url` text NOT NULL,
  `starttime` bigint(20) NOT NULL COMMENT 'timestamp in milliseconds',
  `endtime` bigint(20) NOT NULL COMMENT 'timestamp in milliseconds',
  `step` int(11) NOT NULL COMMENT 'step to be used to break down task into smaller time periods, in milliseconds',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1 COMMENT='high level tasks to scrape trades/markets for certain time periods';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2019-04-30  8:05:40
