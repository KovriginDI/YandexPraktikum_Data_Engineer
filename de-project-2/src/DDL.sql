CREATE TABLE PUBLIC.SHIPPING_COUNTRY_RATES (
    SHIPPING_COUNTRY_ID SERIAL PRIMARY KEY,
    SHIPPING_COUNTRY TEXT NULL,
    SHIPPING_COUNTRY_BASE_RATE NUMERIC(14, 3) NULL
);

CREATE TABLE PUBLIC.SHIPPING_AGREEMENT (
    AGREEMENTID BIGINT PRIMARY KEY,
    AGREEMENT_NUMBER TEXT,
    AGREEMENT_RATE NUMERIC(14, 3),
    AGREEMENT_COMMISSION NUMERIC(14, 3) 
);

CREATE TABLE PUBLIC.SHIPPING_TRANSFER (
    TRANSFER_TYPE_ID SERIAL PRIMARY KEY,
    TRANSFER_TYPE TEXT,
    TRANSFER_MODEL TEXT,
    SHIPPING_TRANSFER_RATE NUMERIC(14, 3) 
);

CREATE TABLE PUBLIC.SHIPPING_INFO (
    SHIPPINGID INT8 PRIMARY KEY,
    VENDORID INT8 NULL,
    PAYMENT_AMOUNT NUMERIC(14, 2) NULL, 
    SHIPPING_PLAN_DATETIME TIMESTAMP NULL, 
    TRANSFER_TYPE_ID INT8 REFERENCES PUBLIC.SHIPPING_TRANSFER (TRANSFER_TYPE_ID) ON UPDATE CASCADE,
    SHIPPING_COUNTRY_ID INT8 REFERENCES PUBLIC.SHIPPING_COUNTRY_RATES (SHIPPING_COUNTRY_ID) ON UPDATE CASCADE,
    AGREEMENTID INT8 REFERENCES PUBLIC.SHIPPING_AGREEMENT (AGREEMENTID) ON UPDATE CASCADE
);

CREATE TABLE PUBLIC.SHIPPING_STATUS (
    SHIPPINGID INT8 PRIMARY KEY,
    STATUS TEXT NULL,
    STATE TEXT NULL,
    SHIPPING_START_FACT_DATETIME TIMESTAMP NULL,
    SHIPPING_END_FACT_DATETIME TIMESTAMP NULL
);
