-- Verificar duplicados na tabela gps_sentido
SELECT ordem, token, COUNT(*) as count
FROM public.gps_sentido
GROUP BY ordem, token
HAVING COUNT(*) > 1;

-- Limpar duplicados mantendo apenas o mais recente
DELETE FROM public.gps_sentido
WHERE ctid NOT IN (
    SELECT max(ctid)
    FROM public.gps_sentido
    GROUP BY ordem, token
);

-- Adicionar constraint se não existir
ALTER TABLE public.gps_sentido 
ADD CONSTRAINT IF NOT EXISTS gps_sentido_pkey PRIMARY KEY (ordem, token);
