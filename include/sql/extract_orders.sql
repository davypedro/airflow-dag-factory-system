-- =============================================================
-- Extract Orders
-- =============================================================
-- Extrai pedidos criados nos últimos :days_back dias.
--
-- Parâmetros:
--   :days_back (int) — janela de extração em dias
--
-- Notas:
--   - Exclui pedidos com status 'draft' (não confirmados)
--   - Inclui informações desnormalizadas do cliente para
--     evitar join desnecessário na camada de transformação
-- =============================================================

SELECT
    o.order_id,
    o.order_dt,
    o.status,
    o.tot_amt,
    o.discount_amount,
    o.notes,
    o.internal_flag,
    o.legacy_id,
    c.cust_id,
    c.customer_name,
    c.customer_email,
    c.customer_segment
FROM
    orders o
    INNER JOIN customers c ON c.customer_id = o.customer_id
WHERE
    o.order_dt >= CURRENT_DATE - INTERVAL ':days_back days'
    AND o.status <> 'draft'
ORDER BY
    o.order_dt DESC
