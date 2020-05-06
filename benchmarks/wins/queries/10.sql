SELECT AVG(CAST(wins_4.nfr1 AS double)) AS avgnfr1ok,   AVG(CAST(wins_4.nfr2 AS double)) AS avgnfr2ok,   AVG(CAST(wins_4.nfr3 AS double)) AS avgnfr3ok FROM wins_4 WHERE ((wins_4.ttrk3 NOT IN ('MD', 'RET', 'PLN', 'ONE', 'BCF', 'WRD', 'HST', 'HPO', 'PRV', 'ASD', 'CLS', 'FAR', 'TIL', 'GPR', 'ELK', 'ARP', 'SWF', 'EMD', 'CAS', 'CBY', 'SDY', 'ZIA', 'CWF', 'RIL', 'RP', 'BKF', 'FON', 'ALB', 'FER', 'SRP', 'RUI', 'DEP', 'ELY', 'GF', 'EMT', 'GIL', 'MED', 'ABT', 'PHA', 'BOI', 'OTC', 'UN', 'LNN', 'SUD', 'LBG', 'WYO', 'SON', 'MIL', 'FTP', 'GRP', 'FMT', 'CPW')) AND (wins_4.ttrk3 = 'GP')) HAVING (COUNT(1) > 0);