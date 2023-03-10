CREATE TABLE DDS.DM_RESTAURANTS
(
    ID SERIAL PRIMARY KEY NOT NULL,
    RESTAURANT_ID VARCHAR NOT NULL,
    NAME VARCHAR NOT NULL
);

CREATE TABLE DDS.DM_COURIERS
(
    ID SERIAL PRIMARY KEY NOT NULL,
    COURIER_ID VARCHAR NOT NULL,
    NAME VARCHAR NOT NULL
);


CREATE TABLE DDS.DM_ORDERS
(
    ID SERIAL PRIMARY KEY NOT NULL,
    ORDER_ID VARCHAR NOT NULL,
    ORDER_TS TIMESTAMP(0),
    DELIVERY_ID VARCHAR NOT NULL,
    SUM INT NOT NULL
);

CREATE TABLE DDS.DM_DELIVERIES
(
    ID SERIAL PRIMARY KEY NOT NULL,
    DELIVERY_ID VARCHAR NOT NULL,
    DELIVERY_TS TIMESTAMP(0),
    COURIER_ID VARCHAR NOT NULL,
    ADDRESS VARCHAR NOT NULL,
    RATE INT NOT NULL,
    TIP_SUM INT NOT NULL
);