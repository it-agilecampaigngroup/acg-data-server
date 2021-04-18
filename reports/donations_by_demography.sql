SELECT SUM(d.amount) total_amount
, ROUND(SUM(d.amount)/SUM(d.response_count), 2) average_amount
, SUM(d.yes_count) yes_count
, SUM(d.no_count) no_count
, SUM(d.response_count) response_count
-- Under 10k
, ROUND(SUM(d.under_10k_amount), 2) under_10k_amount
, ROUND(SUM(d.under_10k_amount)/SUM(d.response_count), 2) under_10k_average
, ROUND(SUM(d.under_10k_yes_count)) under_10k_yes_count
, ROUND(SUM(d.under_10k_no_count)) under_10k_no_count
-- 10K to 24K
, ROUND(SUM(d.ten_to_24k_amount), 2) ten_to_24k_amount
, ROUND(SUM(d.ten_to_24k_amount)/SUM(d.response_count), 2) ten_to_24k_average
, ROUND(SUM(d.ten_to_24k_yes_count)) ten_to_24k_yes_count
, ROUND(SUM(d.ten_to_24k_no_count)) ten_to_24k_no_count
-- 25K to 49K
, ROUND(SUM(d.twenty_five_to_49k_amount), 2) twenty_five_to_49k_amount
, ROUND(SUM(d.twenty_five_to_49k_amount)/SUM(d.response_count), 2) twenty_five_to_49k_average
, ROUND(SUM(d.twenty_five_to_49k_yes_count)) twenty_five_to_49k_yes_count
, ROUND(SUM(d.twenty_five_to_49k_no_count)) twenty_five_to_49k_no_count
-- 50K to 74K
, ROUND(SUM(d.fifty_to_74k_amount), 2) fifty_to_74k_amount
, ROUND(SUM(d.fifty_to_74k_amount)/SUM(d.response_count), 2) fifty_to_74k_average
, ROUND(SUM(d.fifty_to_74k_yes_count)) fifty_to_74k_yes_count
, ROUND(SUM(d.fifty_to_74k_no_count)) fifty_to_74k_no_count
-- 75K and up
, ROUND(SUM(d.seventy_five_and_up_amount), 2) seventy_five_and_up_amount
, ROUND(SUM(d.seventy_five_and_up_amount)/SUM(d.response_count), 2) seventy_five_and_up_average
, ROUND(SUM(d.seventy_five_and_up_yes_count)) seventy_five_and_up_yes_count
, ROUND(SUM(d.seventy_five_and_up_no_count)) seventy_five_and_up_no_count
FROM (
    SELECT donations.amount, donations.yes_count, donations.no_count, donations.response_count
    -- Under 10K
	, donations.amount * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_amount
	, donations.yes_count * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_yes_count
	, donations.no_count * CAST(income.est_under_10k_pop as decimal) / income.est_income_total_pop under_10k_no_count
-- 10K to 24K
	, donations.amount * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_amount
	, donations.yes_count * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_yes_count
	, donations.no_count * CAST(income.est_10_to_14k_pop + income.est_15_to_19k_pop + income.est_20_to_24k_pop as decimal) / income.est_income_total_pop ten_to_24k_no_count
-- 25K to 49K
	, donations.amount * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_amount
	, donations.yes_count * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_yes_count
	, donations.no_count * CAST(income.est_25_to_29k_pop + income.est_30_to_34k_pop + income.est_35_to_39k_pop + income.est_40_to_44k_pop + income.est_45_to_49k_pop as decimal) / income.est_income_total_pop twenty_five_to_49k_no_count
-- 50K to 74K
	, donations.amount * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_amount
	, donations.yes_count * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_yes_count
	, donations.no_count * CAST(income.est_50_to_59k_pop + income.est_60_to_74k_pop as decimal) / income.est_income_total_pop fifty_to_74k_no_count
-- 75K and up
	, donations.amount * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_amount
	, donations.yes_count * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_yes_count
	, donations.no_count * CAST(income.est_75_to_99k_pop + income.est_100_to_124k_pop + income.est_125_to_149k_pop + income.est_150_to_199k_pop + income.est_200k_and_up_pop as decimal) / income.est_income_total_pop seventy_five_and_up_no_count
    FROM (
        SELECT CAST(cal.detail ->> 'personId' AS bigint) person_id
        , SUM(CASE WHEN cal.detail ->> 'amount' <> '' THEN CAST(cal.detail ->> 'amount' AS decimal) ELSE 0 END) amount
        , 1 response_count
        , CASE WHEN cres.description ILIKE 'Positive response' THEN 1 ELSE 0 END yes_count
        , CASE WHEN cres.description NOT ILIKE 'Positive response' THEN 1 ELSE 0 END no_count
        FROM base.contact_action_log cal
        INNER JOIN base.contact_action ca ON ca.action_id = cal.contact_action_id
        INNER JOIN base.contact_reason cr ON cr.reason_id = cal.contact_reason_id
        INNER JOIN base.contact_method cm ON cm.method_id = cal.contact_method_id
        INNER JOIN base.contact_result cres ON cres.result_id = cal.contact_result_id
        WHERE ca.description ILIKE 'Contact responded'
        AND cr.description ILIKE 'Donation request'
        AND cal.campaign_id = 1
        GROUP BY person_id, cres.description
        ) donations
    INNER JOIN base.person_address pa ON pa.person_id = donations.person_id AND pa.is_primary = true
    INNER JOIN base.address addr ON addr.address_id = pa.address_id
    INNER JOIN census.census_block blk ON blk.block_id = addr.census_block_id
	INNER JOIN census.acs_income_2017 income ON income.block_group_id = blk.block_group_id
) d
;
