CREATE OR REPLACE VIEW PUBLIC.SHIPPING_DATAMART AS
(
SELECT
    I.SHIPPINGID,
    I.VENDORID,
    T.TRANSFER_TYPE,
    EXTRACT(DAY FROM (S.SHIPPING_END_FACT_DATETIME - S.SHIPPING_START_FACT_DATETIME)) AS FULL_DAY_AT_SHIPPING,
    CASE 
	    WHEN S.SHIPPING_END_FACT_DATETIME > I.SHIPPING_PLAN_DATETIME and S.STATUS = 'finished' THEN 1
	    WHEN S.SHIPPING_END_FACT_DATETIME <= I.SHIPPING_PLAN_DATETIME and S.STATUS = 'finished' THEN 0
        ELSE NULL
    END AS IS_DELAY,
    CASE 
	    WHEN S.STATUS = 'finished' THEN 1
        ELSE 0
    END AS IS_SHIPPING_FINISH,
    CASE 
	    WHEN S.SHIPPING_END_FACT_DATETIME > I.SHIPPING_PLAN_DATETIME then EXTRACT(DAY FROM (S.SHIPPING_END_FACT_DATETIME - I.SHIPPING_PLAN_DATETIME))
        ELSE 0
    END AS DELAY_DAY_AT_SHIPPING,
    I.PAYMENT_AMOUNT,
    I.PAYMENT_AMOUNT * (R.SHIPPING_COUNTRY_BASE_RATE + A.AGREEMENT_RATE + T.SHIPPING_TRANSFER_RATE) AS VAT,
    I.PAYMENT_AMOUNT * A.AGREEMENT_COMMISSION AS PROFIT
FROM PUBLIC.SHIPPING_INFO I 
JOIN PUBLIC.SHIPPING_STATUS S 
    ON I.SHIPPINGID = S.SHIPPINGID
JOIN PUBLIC.SHIPPING_TRANSFER T 
    ON I.TRANSFER_TYPE_ID = T.TRANSFER_TYPE_ID
JOIN PUBLIC.SHIPPING_COUNTRY_RATES R 
    ON I.SHIPPING_COUNTRY_ID = R.SHIPPING_COUNTRY_ID
JOIN PUBLIC.SHIPPING_AGREEMENT A 
    ON I.AGREEMENTID = A.AGREEMENTID
);
