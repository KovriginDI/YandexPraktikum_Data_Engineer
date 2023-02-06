CREATE TABLE ANALYSIS.DM_RFM_SEGMENTS
(
USER_ID INT NOT NULL REFERENCES PRODUCTION.USERS(ID),
RECENCY INT NOT NULL CHECK (RECENCY BETWEEN 1 AND 5),
FREQUENCY INT NOT NULL CHECK (FREQUENCY BETWEEN 1 AND 5), 
MONETARY_VALUE INT NOT NULL CHECK (MONETARY_VALUE BETWEEN 1 AND 5),
PRIMARY KEY (USER_ID)
)
;