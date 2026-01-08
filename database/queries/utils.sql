-- Checar atualização rio e angra tabela gps_posicoes

SELECT
    max(to_timestamp(datahora / 1000) AT TIME ZONE 'America/Sao_Paulo') as max_date
FROM public.gps_posicoes_rio
union  all 
select
max(event_date) as max_date
from public.gps_posicoes_angra;


