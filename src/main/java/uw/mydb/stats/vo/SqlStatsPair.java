package uw.mydb.stats.vo;

/**
 * sql统计数据对。
 * 里面包含两个sql统计对象，一个是全局对象，一个是临时对象。
 *
 * @author axeon
 */
public class SqlStatsPair {

    public SqlStats sqlStats;

    public SqlStats sqlStatsMetrics;

    public SqlStatsPair(boolean enableStats, boolean enableMetrics) {
        if (enableStats) {
            sqlStats = new SqlStats();
            if (enableMetrics) {
                sqlStatsMetrics = new SqlStats();
            }
        }
    }

    public void addSqlReadCount(long sqlCount) {
        if (sqlStats != null) {
            sqlStats.addSqlReadCount(sqlCount);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addSqlReadCount(sqlCount);
            }
        }
    }

    public void addSqlWriteCount(long sqlCount) {
        if (sqlStats != null) {
            sqlStats.addSqlWriteCount(sqlCount);

            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addSqlWriteCount(sqlCount);
            }
        }
    }

    public void addExeSuccessCount(long exeSuccessCount) {
        if (sqlStats != null) {
            sqlStats.addExeSuccessCount(exeSuccessCount);

            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addExeSuccessCount(exeSuccessCount);
            }
        }
    }

    public void addExeFailureCount(long exeFailureCount) {
        if (sqlStats != null) {
            sqlStats.addExeFailureCount(exeFailureCount);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addExeFailureCount(exeFailureCount);
            }
        }
    }

    public void addDataRowsCount(long dataRowsCount) {
        if (sqlStats != null) {
            sqlStats.addDataRowsCount(dataRowsCount);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addDataRowsCount(dataRowsCount);
            }
        }
    }

    public void addAffectRowsCount(long affectRowsCount) {
        if (sqlStats != null) {
            sqlStats.addAffectRowsCount(affectRowsCount);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addAffectRowsCount(affectRowsCount);
            }
        }
    }

    public void addExeTime(long exeTime) {
        if (sqlStats != null) {
            sqlStats.addExeTime(exeTime);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addExeTime(exeTime);
            }
        }
    }

    public void addSendBytes(long sendBytes) {
        if (sqlStats != null) {
            sqlStats.addSendBytes(sendBytes);
            if (sqlStatsMetrics != null) {
                sqlStatsMetrics.addSendBytes(sendBytes);
            }
        }
    }

    public void addRecvBytes(long recvBytes) {
        if (sqlStats != null) {
            sqlStats.addRecvBytes(recvBytes);
            if (sqlStatsMetrics != null) {
                sqlStats.addRecvBytes(recvBytes);
            }
        }
    }

    public long getSqlReadCount() {
        if (sqlStats != null) {
            return sqlStats.getSqlReadCount();
        } else {
            return -1;
        }
    }

    public long getAndClearSqlReadCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearSqlReadCount();
        } else {
            return -1;
        }
    }

    public long getSqlWriteCount() {
        if (sqlStats != null) {
            return sqlStats.getSqlWriteCount();
        } else {
            return -1;
        }
    }

    public long getAndClearSqlWriteCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearSqlWriteCount();
        } else {
            return -1;
        }
    }

    public long getDataRowsCount() {
        if (sqlStats != null) {
            return sqlStats.getDataRowsCount();
        } else {
            return -1;
        }
    }

    public long getAndClearDataRowsCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearDataRowsCount();
        } else {
            return -1;
        }
    }

    public long getAffectRowsCount() {
        if (sqlStats != null) {
            return sqlStats.getAffectRowsCount();
        } else {
            return -1;
        }
    }

    public long getAndClearAffectRowsCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearAffectRowsCount();
        } else {
            return -1;
        }
    }

    public long getExeSuccessCount() {
        if (sqlStats != null) {
            return sqlStats.getExeSuccessCount();
        } else {
            return -1;
        }
    }

    public long getAndClearExeSuccessCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearExeSuccessCount();
        } else {
            return -1;
        }
    }

    public long getAndClearExeFailureCount() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearExeFailureCount();
        } else {
            return -1;
        }
    }

    public long getExeFailureCount() {
        if (sqlStats != null) {
            return sqlStats.getExeFailureCount();
        } else {
            return -1;
        }
    }

    public long getExeTime() {
        if (sqlStats != null) {
            return sqlStats.getExeTime();
        } else {
            return -1;
        }
    }

    public long getAndClearExeTime() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearExeTime();
        } else {
            return -1;
        }
    }

    public long getSendBytes() {
        if (sqlStats != null) {
            return sqlStats.getSendBytes();
        } else {
            return -1;
        }
    }

    public long getAndClearSendBytes() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearSendBytes();
        } else {
            return -1;
        }
    }

    public long getRecvBytes() {
        if (sqlStats != null) {
            return sqlStats.getRecvBytes();
        } else {
            return -1;
        }
    }

    public long getAndClearRecvBytes() {
        if (sqlStatsMetrics != null) {
            return sqlStatsMetrics.getAndClearRecvBytes();
        } else {
            return -1;
        }
    }
}
