SELECT CD_ELEICAO AS idEleicao,
       SQ_CANDIDATO AS idCandidatura,
       NR_ORDEM_BEM_CANDIDATO AS nrOrdemBemCandidato,
       DS_TIPO_BEM_CANDIDATO AS descTipoBemCandidato,
       DS_BEM_CANDIDATO AS descBemCandidato,
       double(REPLACE(VR_BEM_CANDIDATO, ',', '.')) AS vlBemCandidato

FROM bronze.tse.bem_candidato