# PhonePe Pulse - Data

The Indian digital payments story has truly captured the world’s imagination. From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-art payments infrastructure built as Public Goods championed by the central bank and the government. PhonePe started in 2016 and has been a strong beneficiary of the API driven digitisation of payments in India. When we started , we were constantly looking for definitive data sources on digital payments in India without much success. As a way of giving back to the data and developer community, we decided to open the anonymised aggregate data sets that demystify the what, why and how of digital payments in India. Licensed under the [CDLA-Permissive-2.0 open data license](https://github.com/PhonePe/pulse/blob/master/LICENSE), the PhonePe Pulse Dataset API is a first of its kind open data initiative in the payments space.

## Installaton Files 
- streamlit
- streamlit_option_menu
- seaborn
- matplotlib
- pandas
- streamlit_plotly_events
- plotly
- math


## Table of Contents

<!-- TOC -->
 Pages Details
    - Home 
    - Decoding Transaction Dynamics on PhonePe 
    - Device Dominance and User Engagement 
    - Analysis Insurance Transactions Analysis
    - User Registration Analysis
    - Transaction Analysis Across States and Districts

<!-- /TOC -->

## Dataset 
  1. Dataset available in Git Link https://github.com/PhonePe/pulse/tree/master/data

# SQL Database 
  CREATE TABLE IF NOT EXISTS public.aggregated_insurance
    (
        insurance_id integer NOT NULL DEFAULT nextval('aggregated_insurance_insurance_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater bigint,
        insurance_type character varying(50) COLLATE pg_catalog."default",
        insurance_count bigint,
        insurance_amount double precision,
        CONSTRAINT aggregated_insurance_pkey PRIMARY KEY (insurance_id)
    )

    CREATE TABLE IF NOT EXISTS public.aggregated_transaction
    (
        trans_id integer NOT NULL DEFAULT nextval('aggregated_transaction_trans_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        transaction_type character varying(50) COLLATE pg_catalog."default",
        transaction_count bigint,
        transaction_amount double precision,
        CONSTRAINT aggregated_transaction_pkey PRIMARY KEY (trans_id)
    )
    CREATE TABLE IF NOT EXISTS public.aggregated_user
    (
        user_id integer NOT NULL DEFAULT nextval('aggregated_user_user_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quarter integer,
        brand character varying(50) COLLATE pg_catalog."default",
        brand_count bigint,
        brand_percentage double precision,
        registered_users bigint,
        app_opens bigint,
        CONSTRAINT aggregated_user_pkey PRIMARY KEY (user_id)
    )
    CREATE TABLE IF NOT EXISTS public.map_insurance
    (
        insurance_id integer NOT NULL DEFAULT nextval('map_insurance_insurance_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        trans_district character varying(50) COLLATE pg_catalog."default",
        districttrans_count integer,
        districttrans_amount double precision,
        CONSTRAINT map_insurance_pkey PRIMARY KEY (insurance_id)
    )
    CREATE TABLE IF NOT EXISTS public.map_transaction
    (
        trans_id integer NOT NULL DEFAULT nextval('map_transaction_trans_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        insurance_district character varying(50) COLLATE pg_catalog."default",
        districtinsurance_count integer,
        districtinsurancet_amount double precision,
        CONSTRAINT map_transaction_pkey PRIMARY KEY (trans_id)
    )
    CREATE TABLE IF NOT EXISTS public.map_user
    (
        user_id integer NOT NULL DEFAULT nextval('map_user_user_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        user_district character varying(50) COLLATE pg_catalog."default",
        registeredusers_count integer,
        CONSTRAINT map_user_pkey PRIMARY KEY (user_id)
    )
    CREATE TABLE IF NOT EXISTS public.top_insurance
    (
        insurance_id integer NOT NULL DEFAULT nextval('top_insurance_insurance_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        insurance_type character varying(50) COLLATE pg_catalog."default",
        insurance_count bigint,
        insurance_amount double precision,
        CONSTRAINT top_insurance_pkey PRIMARY KEY (insurance_id)
    )
    CREATE TABLE IF NOT EXISTS public.top_transaction
    (
        trans_id integer NOT NULL DEFAULT nextval('top_transaction_trans_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        transaction_type character varying(50) COLLATE pg_catalog."default",
        transaction_count bigint,
        transaction_amount double precision,
        CONSTRAINT top_transaction_pkey PRIMARY KEY (trans_id)
    )
    CREATE TABLE IF NOT EXISTS public.top_user
    (
        user_id integer NOT NULL DEFAULT nextval('top_user_user_id_seq'::regclass),
        state character varying(50) COLLATE pg_catalog."default" NOT NULL,
        year character varying(15) COLLATE pg_catalog."default",
        quater integer,
        user_type character varying(50) COLLATE pg_catalog."default",
        registeredusers_count integer,
        CONSTRAINT top_user_pkey PRIMARY KEY (user_id)
    )

