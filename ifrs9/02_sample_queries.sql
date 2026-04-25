-- IFRS 9 sample queries demonstrating stage classification logic and ECL calc.

-- ============================================================================
-- Q1. Current stage per account (latest stage_event wins).
-- ============================================================================
with latest as (
  select
    account_id,
    max(as_of_date) as max_date
  from ifrs9.stage_event
  group by account_id
)
select se.account_id, se.as_of_date, se.new_stage as current_stage, se.reason_code
from ifrs9.stage_event se
join latest l
  on l.account_id = se.account_id
 and l.max_date  = se.as_of_date
order by se.account_id;

-- ============================================================================
-- Q2. Stage classification rule (launch baseline):
--   - DPD >= 90 OR status = written_off => S3
--   - DPD 30..89 OR SICR flag => S2
--   - otherwise S1
-- This query proposes the correct stage per account for a given as_of_date
-- based on the latest exposure snapshot. Use it to generate stage_event rows.
-- ============================================================================
select
  e.account_id,
  e.as_of_date,
  case
    when a.status = 'written_off' or e.days_past_due >= 90 then 'S3'::ifrs9.stage_code
    when e.days_past_due >= 30                             then 'S2'::ifrs9.stage_code
    else                                                        'S1'::ifrs9.stage_code
  end as proposed_stage
from ifrs9.exposure_snapshot e
join ifrs9.account a using (account_id)
where e.as_of_date = :as_of_date;

-- ============================================================================
-- Q3. Pick-the-right-PD helper: for a given (segment, stage, basis, as_of_date),
--     return the PD whose effective window covers the as_of_date.
-- ============================================================================
select pd_param_id, pd_value, horizon_months
from ifrs9.pd_parameter
where segment_id = :segment_id
  and stage      = :stage::ifrs9.stage_code
  and basis      = :basis::ifrs9.param_basis
  and effective_from <= :as_of_date
  and (effective_to is null or effective_to > :as_of_date)
order by effective_from desc
limit 1;

-- ============================================================================
-- Q4. ECL calculation per account for a given as_of_date + run_id.
--     ECL = PD * LGD * EAD  (simplified; lifetime vs 12m controlled by PD horizon)
--     EAD = outstanding_principal + accrued_profit + ccf * undrawn_commitment
--     Apply scenario-weighted macro overlays to PD and LGD, then weight by scenario.
-- ============================================================================
with
  current_stage as (
    select distinct on (account_id)
      account_id, as_of_date, new_stage as stage
    from ifrs9.stage_event
    where as_of_date <= :as_of_date
    order by account_id, as_of_date desc
  ),
  segment as (
    select distinct on (account_id)
      account_id, segment_id
    from ifrs9.account_segment_assignment
    where effective_from <= :as_of_date
      and (effective_to is null or effective_to > :as_of_date)
    order by account_id, effective_from desc
  ),
  exposure as (
    select *
    from ifrs9.exposure_snapshot
    where as_of_date = :as_of_date
  ),
  pd_pit as (
    select segment_id, stage, basis, pd_value, pd_param_id
    from ifrs9.pd_parameter
    where basis = 'PIT'
      and effective_from <= :as_of_date
      and (effective_to is null or effective_to > :as_of_date)
  ),
  lgd_pit as (
    select segment_id, basis, lgd_value, lgd_param_id
    from ifrs9.lgd_parameter
    where basis = 'PIT'
      and effective_from <= :as_of_date
      and (effective_to is null or effective_to > :as_of_date)
  ),
  scenarios as (
    select s.scenario_id, s.weight, o.segment_id, o.pd_multiplier, o.lgd_multiplier
    from ifrs9.macro_scenario s
    join ifrs9.macro_overlay o on o.scenario_id = s.scenario_id
    where s.effective_from <= :as_of_date
      and (s.effective_to is null or s.effective_to > :as_of_date)
      and o.effective_from <= :as_of_date
      and (o.effective_to is null or o.effective_to > :as_of_date)
  ),
  per_scenario as (
    select
      e.account_id,
      cs.stage,
      seg.segment_id,
      sc.scenario_id,
      sc.weight,
      pd.pd_value * sc.pd_multiplier    as pd_adj,
      lg.lgd_value * sc.lgd_multiplier  as lgd_adj,
      (e.outstanding_principal + e.accrued_profit + e.ccf * e.undrawn_commitment) as ead,
      pd.pd_param_id,
      lg.lgd_param_id
    from exposure e
    join current_stage cs on cs.account_id = e.account_id
    join segment seg      on seg.account_id = e.account_id
    join pd_pit pd        on pd.segment_id = seg.segment_id and pd.stage = cs.stage
    join lgd_pit lg       on lg.segment_id = seg.segment_id
    join scenarios sc     on sc.segment_id = seg.segment_id
  )
select
  account_id,
  stage,
  segment_id,
  sum(weight * pd_adj * lgd_adj * ead)  as ecl_amount,
  jsonb_object_agg(scenario_id, weight) as scenario_weights,
  max(pd_param_id)  as pd_param_id,
  max(lgd_param_id) as lgd_param_id,
  max(ead)          as ead_used
from per_scenario
group by account_id, stage, segment_id;

-- ============================================================================
-- Q5. Persist the calculated ECL into the audit table (one batch run).
-- ============================================================================
insert into ifrs9.provision_calculation (
  account_id, as_of_date, stage, segment_id,
  pd_param_id, lgd_param_id, pd_used, lgd_used, ead_used,
  scenario_weights, ecl_amount, run_id
)
select
  account_id, :as_of_date::date, stage, segment_id,
  pd_param_id, lgd_param_id,
  -- Weighted PD/LGD: document what we used, not just the product.
  (select sum(weight * pd_adj) / nullif(sum(weight),0) from per_scenario p2 where p2.account_id = q.account_id),
  (select sum(weight * lgd_adj) / nullif(sum(weight),0) from per_scenario p2 where p2.account_id = q.account_id),
  ead_used,
  scenario_weights, ecl_amount,
  :run_id::uuid
from ( /* Q4 output aliased */ ) q;

-- ============================================================================
-- Q6. Reconcile month-over-month ECL movement (for explainability).
-- ============================================================================
with cur as (
  select account_id, sum(ecl_amount) as ecl_now
  from ifrs9.provision_calculation
  where as_of_date = :as_of_date and run_id = :run_id::uuid
  group by account_id
),
prv as (
  select account_id, sum(ecl_amount) as ecl_prev
  from ifrs9.provision_calculation
  where as_of_date = (:as_of_date::date - interval '1 month')::date
  group by account_id
)
select coalesce(cur.account_id, prv.account_id) as account_id,
       coalesce(prv.ecl_prev, 0) as ecl_prev,
       coalesce(cur.ecl_now,  0) as ecl_now,
       coalesce(cur.ecl_now,  0) - coalesce(prv.ecl_prev, 0) as delta
from cur full outer join prv using (account_id)
order by abs(delta) desc;
