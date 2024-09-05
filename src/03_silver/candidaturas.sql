SELECT CD_ELEICAO AS idEleicao,
       SG_UF AS descUF,
       SG_UE AS idUnidadeEleitoral,
       NM_UE AS descUnidadeEleitoral,
       DS_CARGO AS descCargoCandidatura,
       SQ_CANDIDATO AS idCandidatura,
       NR_CANDIDATO AS nrCandidato,
       NM_CANDIDATO AS descNomeCandidato,
       NM_URNA_CANDIDATO AS nrUrnaCandidatura,
       TP_AGREMIACAO AS descTipoAgremiacao,
       NR_PARTIDO AS nrPartido,
       SG_PARTIDO AS descSiglaPartido,
       NM_PARTIDO AS descNomePartido,
       NM_FEDERACAO AS descNomeFederacao,
       NM_COLIGACAO AS descNomeColigacao,
       DS_COMPOSICAO_COLIGACAO AS descComposicaoColigacao,
       SG_UF_NASCIMENTO AS descSiglaUFNascimentoCandidato,
       DT_NASCIMENTO AS dtNascimentoCandidato,
       NR_TITULO_ELEITORAL_CANDIDATO AS nrTituloEleitoralCandidato,
       DS_GENERO AS descGeneroCandidato,
       DS_GRAU_INSTRUCAO AS descGrauInstrucaoCandidato,
       DS_ESTADO_CIVIL AS descEstadoCivilCandidato,
       DS_COR_RACA AS descCorRacaCandidato,
       DS_OCUPACAO AS descOcupacaoCandidato

FROM bronze.tse.consulta_cand