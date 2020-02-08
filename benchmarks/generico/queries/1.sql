SELECT generico_1.categoria AS categoria FROM generico_1 WHERE ((generico_1.anunciante IN ('BANTRAB/TODOTICKET', 'TODOTICKET', 'TODOTICKET.COM')) AND (CAST(EXTRACT(YEAR FROM generico_1.fecha) AS LONG) >= 2010) AND (CAST(EXTRACT(YEAR FROM generico_1.fecha) AS LONG) <= 2015)) GROUP BY generico_1.categoria ORDER BY categoria ASC ;