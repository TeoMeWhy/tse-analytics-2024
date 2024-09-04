WITH tb_cand AS (

    SELECT  DISTINCT
            SQ_CANDIDATO,
            SG_UF,
            DS_CARGO,
            SG_PARTIDO,
            NM_PARTIDO,
            DT_NASCIMENTO,
            INT(months_between(NOW(), DT_NASCIMENTO) / 12) AS NR_IDADE,
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
           sum(double(replace( VR_BEM_CANDIDATO, ',', '.'))) AS totalBens
    FROM bronze.tse.bem_candidato
    WHERE NOT (SQ_CANDIDATO = 160002381679 AND DS_BEM_CANDIDATO = 'VEICULOS')
    GROUP BY 1
),

tb_info_completa_cand AS (

    SELECT t1.*,
        COALESCE(t2.totalBens, 0) AS totalBens        

    FROM tb_cand AS t1
    LEFT JOIN tb_total_bens AS t2
    ON t1.SQ_CANDIDATO = t2.SQ_CANDIDATO

),

tb_cube AS (
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
            MEDIAN(totalBens) AS medianBens,
            COALESCE(MEDIAN(case when totalBens > 1 then totalBens end),0) AS medianBensNotZero,
            1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL='CASADO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilCasado,
            1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL='SOLTEIRO(A)' THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSolteiro,
            1.0 * SUM(CASE WHEN DS_ESTADO_CIVIL IN ('DIVORCIADO(A))', 'SEPARADO(A) JUDICIALMENTE') THEN 1 ELSE 0 END) / count(*) AS txEstadoCivilSeparadoDivorciado,
            AVG(NR_IDADE) AS avgIdade,
            count(*) AS totalCandidaturas

    FROM tb_info_completa_cand

    GROUP BY SG_PARTIDO, DS_CARGO, SG_UF WITH CUBE
    ORDER BY SG_PARTIDO, DS_CARGO, SG_UF
),

tb_final AS (

    SELECT
        coalesce(SG_PARTIDO, 'GERAL') AS SG_PARTIDO,
        coalesce(DS_CARGO, 'GERAL') AS DS_CARGO,
        coalesce(SG_UF, 'BR') AS SG_UF,
        txGenFeminino,
        totalGenFeminino,
        txCorRacaPreta,
        totalCorRacaPreta,
        txCorRacaPretaParda,
        totalCorRacaPretaParda,
        txCorRacaNaoBranca,
        totalCorRacaNaoBranca,
        totalBens,
        avgBens,
        avgBensNotZero,
        totalBens / 1000 AS totalBensPer1000,
        avgBens / 1000 AS avgBensPer1000,
        avgBensNotZero / 1000 AS avgBensPerNotZero1000,
        medianBens,
        medianBensNotZero,
        medianBens / 1000 AS medianBensPer1000,
        medianBensNotZero / 1000 AS medianBensNotZeroPer1000,
        txEstadoCivilCasado,
        txEstadoCivilSolteiro,
        txEstadoCivilSeparadoDivorciado,
        avgIdade,
        totalCandidaturas

    FROM tb_cube
)

SELECT *
FROM tb_final