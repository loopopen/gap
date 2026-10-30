package xmysql

import "fmt"

const statusColumnType = "enum('Pending','Succeeded','Failed')"

func publishedCreateSQL(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ("+
			"`id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NOT NULL,"+
			"`version` varchar(16) NOT NULL,"+
			"`topic` varchar(256) NOT NULL,"+
			"`status` "+statusColumnType+" NOT NULL,"+
			"`headers` text,"+
			"`payload` longtext,"+
			"`retries` int NOT NULL DEFAULT 0,"+
			"`lease_token` char(32) NULL,"+
			"`lease_until` datetime(3) NULL,"+
			"`next_attempt_at` datetime(3) NOT NULL,"+
			"`expired_at` datetime(3) NULL,"+
			"PRIMARY KEY (`id`)"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table,
	)
}

func receivedCreateSQL(table string) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ("+
			"`id` bigint unsigned AUTO_INCREMENT,"+
			"`created_at` datetime(3) NOT NULL,"+
			"`version` varchar(16) NOT NULL,"+
			"`topic` varchar(256) NOT NULL,"+
			"`status` "+statusColumnType+" NOT NULL,"+
			"`headers` text,"+
			"`payload` longtext,"+
			"`retries` int NOT NULL DEFAULT 0,"+
			"`lease_token` char(32) NULL,"+
			"`lease_until` datetime(3) NULL,"+
			"`next_attempt_at` datetime(3) NOT NULL,"+
			"`expired_at` datetime(3) NULL,"+
			"`group` varchar(128) NOT NULL,"+
			"PRIMARY KEY (`id`)"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table,
	)
}
