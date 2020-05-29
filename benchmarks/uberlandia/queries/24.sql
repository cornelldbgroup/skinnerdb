SELECT uberlandia_1.number_of_records FROM uberlandia_1 WHERE ((uberlandia_1.nome_do_curso IN ('Agente de Sade e Bem estar Animal', 'Eletricista de Veculos de Transporte de Cargas e de Passageiros', 'Recepcionista em Meios de Hospedagem', 'RECEPCIONISTA EM MEIOS DE HOSPEDAGEM', 'Soldador no Processo MIG/MAG', 'SOLDADOR NO PROCESSO MIG/MAG', 'Traador de Caldeiraria', 'TRAADOR DE CALDEIRARIA')) AND (NOT (uberlandia_1.situacao_da_turma IN ('CANCELADA', 'CRIADA', 'PUBLICADA'))) AND (uberlandia_1.subtipo_curso = 'FIC') AND (CAST(EXTRACT(YEAR FROM uberlandia_1.data_de_inicio) AS LONG) = 2015) AND (NOT ((uberlandia_1.nome_da_sit_matricula_situacao_detalhada NOT IN ('', 'TRANSF_EXT', 'INTEGRALIZADA', 'FREQ_INIC_INSUF', 'TRANCADA', 'CONCLUDA', 'TRANSF_INT', 'EM_CURSO', 'REPROVADA', 'ABANDONO', 'CONFIRMADA', 'EM_DEPENDNCIA')) OR (uberlandia_1.nome_da_sit_matricula_situacao_detalhada IS NULL))));
