delete from public.MTDATA_DATA_EVNT_TGT
where trunc(arrivaltimestamp) < current_date -1;

delete from public.MTDATA_DATA_EVNT
where trunc(arrivaltimestamp) < current_date -1;