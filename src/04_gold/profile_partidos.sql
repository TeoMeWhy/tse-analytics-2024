WITH tb_cand AS (

    SELECT  DISTINCT
            SQ_CANDIDATO,
            SG_UF,
            DS_CARGO,
            SG_PARTIDO,
            NM_PARTIDO,
            DT_NASCIMENTO,
            DS_GENERO,
            DS_GRAU_INSTRUCAO,
            DS_ESTADO_CIVIL,
            DS_COR_RACA,
            DS_OCUPACAO

    FROM bronze.tse.consulta_cand
    WHERE ANO_ELEICAO = 2024
),

tb_total_bens AS (
    SELECT SQ_CANDIDATO,
           sum(cast(replace( VR_BEM_CANDIDATO, ',', '.') as DECIMAL(15,2))) AS totalBens
    FROM bronze.tse.bem_candidatos
    GROUP BY 1
),

tb_info_completa_cand AS (

    SELECT t1.*,
        COALESCE(t2.totalBens, 0) AS totalBens,
        date('now') - date(
            substr(DT_NASCIMENTO, 7, 4) || '-' || 
            substr(DT_NASCIMENTO, 4, 2) || '-' || 
            substr(DT_NASCIMENTO, 1, 2)
        ) AS NR_IDADE

    FROM tb_cand AS t1
    LEFT JOIN tb_total_bens AS t2
    ON t1.SQ_CANDIDATO = t2.SQ_CANDIDATO

)

SELECT
        SG_PARTIDO,
        DS_CARGO,
        SG_UF,
        AVG(CASE WHEN DS_GENERO = 'FEMININO' THEN 1 ELSE 0 END) AS txGenFeminino,
        SUM(CASE WHEN DS_GENERO = 'FEMININO' THEN 1 ELSE 0 END) AS totalGenFeminino,
        AVG(CASE WHEN DS_COR_RACA = 'PRETA' THEN 1 ELSE 0 END) AS txCorRacaPreta,
        SUM(CASE WHEN DS_COR_RACA = 'PRETA' THEN 1 ELSE 0 END) AS totalCorRacaPreta,
        AVG(CASE WHEN DS_COR_RACA IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS txCorRacaPretaParda,
        SUM(CASE WHEN DS_COR_RACA IN ('PRETA', 'PARDA') THEN 1 ELSE 0 END) AS totalCorRacaPretaParda,
        AVG(CASE WHEN DS_COR_RACA <> 'BRANCA' THEN 1 ELSE 0 END) AS txCorRacaNaoBranca,
        SUM(CASE WHEN DS_COR_RACA <> 'BRANCA' THEN 1 ELSE 0 END) AS totalCorRacaNaoBranca,
        SUM(totalBens) AS totalBens,
        AVG(totalBens) AS avgBens,
        COALESCE(AVG(case when totalBens > 1 then totalBens end),0) AS avgBensNotZero,
        1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL='CASADO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilCasado,
        1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL='SOLTEIRO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSolteiro,
        1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL IN ('DIVORCIADO(A))', 'SEPARADO(A) JUDICIALMENTE') THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSeparadoDivorciado,
        AVG(NR_IDADE) AS avgIdade,
        count(*) AS totalCandidaturas

FROM tb_info_completa_cand

GROUP BY SG_PARTIDO, DS_CARGO, SG_UF WITH CUBE
ORDER BY SG_PARTIDO, DS_CARGO, SG_UF
