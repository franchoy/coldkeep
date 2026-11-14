-- Exact schema20 fence for untouched supplied-handle v1 writers.
CREATE FUNCTION public.coldkeep_schema20_writer_fence() RETURNS trigger LANGUAGE plpgsql AS $coldkeep$
BEGIN
 IF TG_TABLE_SCHEMA = 'public' AND TG_TABLE_NAME = 'chunk' AND TG_OP = 'UPDATE' THEN
  IF pg_catalog.current_setting('coldkeep.recovery_pin_authority',true) = 'schema20-v1'
  THEN
   IF TG_LEVEL = 'STATEMENT' THEN RETURN NULL; END IF;
   IF NEW.pin_count >= 0 AND pg_catalog.abs(NEW.pin_count - OLD.pin_count) = 1 AND (pg_catalog.to_jsonb(NEW) - 'pin_count') = (pg_catalog.to_jsonb(OLD) - 'pin_count') THEN RETURN NEW; END IF;
  END IF;
 END IF;
 RAISE EXCEPTION 'future version catalog_version 20: ordinary mutations refused';
END
$coldkeep$;
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.schema_version FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.container FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.chunk FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.logical_file FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.physical_file FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.file_chunk FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.repository_config FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.blocks FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.storage_blocks FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.chunk_block_refs FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.snapshot FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.snapshot_path FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.snapshot_file FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.store_repair_attempt FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.store_repair_container FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.store_repair_block FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.store_repair_chunk FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.retired_chunk_block_ref FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.retired_legacy_block_extent FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.repository_identity FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.storage_root_identity FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.device_identity FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.source_identity FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.adoption_operation FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.legacy_snapshot_origin FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.binding_operation FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.root_binding_revision FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.root_binding_head FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.configured_source FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.source_binding_revision FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.source_binding_head FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.schema_upgrade_operation FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE UPDATE OR DELETE OR TRUNCATE ON public.generation_policy FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_fence BEFORE UPDATE OR DELETE OR TRUNCATE ON public.capture_generation FOR EACH STATEMENT EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
CREATE TRIGGER aaaa_coldkeep_schema20_pin_rows BEFORE UPDATE ON public.chunk FOR EACH ROW EXECUTE FUNCTION public.coldkeep_schema20_writer_fence();
