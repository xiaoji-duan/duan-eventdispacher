package com.xiaoji.duan.aag.service.db;

public class CreateTable extends AbstractSql {

	public CreateTable() {
		initDdl();
	}

	private void initDdl() {

		ddl.add("" + 
				"CREATE TABLE IF NOT EXISTS `aag_events` (" +
				"  `UNIONID` varchar(36) NOT NULL," +
				"  `SA_NAME` varchar(256) DEFAULT NULL," +
				"  `SA_PREFIX` varchar(3) NOT NULL," +
				"  `EVENT_ID` varchar(64) NOT NULL," +
				"  `EVENT_TYPE` varchar(64) NOT NULL," +
				"  `EVENT_NAME` varchar(256) NOT NULL," +
				"  `CREATE_TIME` date DEFAULT NULL," +
				"  PRIMARY KEY (`UNIONID`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8;" +
				"");

		ddl.add("" + 
				"CREATE TABLE IF NOT EXISTS `aag_actions` (" +
				"  `UNIONID` varchar(36) NOT NULL," +
				"  `SA_NAME` varchar(256) DEFAULT NULL," +
				"  `SA_PREFIX` varchar(3) NOT NULL," +
				"  `ACTION_ID` varchar(64) NOT NULL," +
				"  `ACTION_TYPE` varchar(64) NOT NULL," +
				"  `ACTION_NAME` varchar(256) NOT NULL," +
				"  `ACTION_RUNWITH` text NOT NULL," +
				"  `CREATE_TIME` date DEFAULT NULL," +
				"  PRIMARY KEY (`UNIONID`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8;" +
				"");

		ddl.add("" + 
				"CREATE TABLE IF NOT EXISTS `aag_tasks` (" +
				"  `UNIONID` varchar(36) NOT NULL," +
				"  `SA_NAME` varchar(256) DEFAULT NULL," +
				"  `SA_PREFIX` varchar(3) NOT NULL," +
				"  `TASK_ID` varchar(128) NOT NULL," +
				"  `TASK_TYPE` varchar(64) NOT NULL," +
				"  `TASK_NAME` varchar(256) NOT NULL," +
                "  `TASK_RUNAT` text NOT NULL," +
                "  `TASK_RUNWITH` text NOT NULL," +
				"  `CREATE_TIME` date DEFAULT NULL," +
				"  PRIMARY KEY (`UNIONID`)" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8;" +
				"");

	}
}
