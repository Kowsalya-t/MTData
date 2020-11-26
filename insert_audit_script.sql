insert into public.interface_process_control
(select trunc(current_date),'mtdata-unload-redshift-s3', max(arrivalTimestamp),  TIMEZONE('UTC',getdate() ) from MTDATA_DATA_EVNT_TGT)