SELECT uberlandia_1.number_of_records FROM uberlandia_1 WHERE ((NOT ((uberlandia_1.nome_da_sit_matricula_situacao_detalhada IN ('CANC_DESISTENTE', 'CANC_MAT_PRIM_OPCAO', 'CANC_SANO', 'CANC_SEM_FREQ_INICIAL', 'CANC_TURMA', 'DOC_INSUFIC', 'ESCOL_INSUFIC', 'INC _ITINERARIO', 'INSC_CANC', 'No Matriculado', 'NO_COMPARECEU', 'TURMA_CANC', 'VAGAS_INSUFIC')) OR (uberlandia_1.nome_da_sit_matricula_situacao_detalhada IS NULL))) AND (NOT (uberlandia_1.situacao_da_turma IN ('CANCELADA', 'CRIADA', 'PUBLICADA'))) AND (CAST(EXTRACT(YEAR FROM uberlandia_1.data_de_inicio) AS LONG) IN (2011, 2012, 2013, 2014, 2015, 2016)));
