SELECT motos_2.number_of_records FROM motos_2 WHERE ((motos_2.medio = 'RADIO') AND (CAST(EXTRACT(YEAR FROM motos_2.fecha) AS LONG) = 2015) AND (motos_2.categoria = 'MOTOCICLETAS'));
