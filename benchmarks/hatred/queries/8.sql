SELECT hatred_1.state_ AS state_ FROM hatred_1 WHERE ((((hatred_1.state_ >= 'AK') AND (hatred_1.state_ <= 'CT')) OR ((hatred_1.state_ >= 'DE') AND (hatred_1.state_ <= 'WY'))) AND (hatred_1.keyword IN ('bimbo', 'hag', 'twat'))) GROUP BY hatred_1.state_ HAVING ((SUM(1) >= 30) AND (SUM(1) <= 100000));